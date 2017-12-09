from socket import *
from _thread import *
import json
import shutil
import random
import time
import sys
# import ballot


class BallotNum:
    def __init__(self, num, ID):
        self.num = num
        self.ID = ID

class Tickets:
    def __init__(self, ID):

        print("System running: " + ID)
        self.ID = ID
        self.port = configdata["kiosks"][ID][1]
        self.processID = int(self.port) - 4000
        self.hostname = gethostname()
        self.BallotNum = BallotNum(0,ID)
        self.AcceptNum = BallotNum(0,0)
        self.AcceptVal = 0
        self.numOfAcks = 0
        self.accepts = 0
        self.leaderport = 0
        self.tickets = 100
        self.leaderIsAlive = False
        w, h = 5, 2 # 4, n-1
        self.acks = [[0 for x in range(w)] for y in range(h)]
        self.acceptances = [[0 for x in range(2)] for y in range(2)] #n-1 rows
        self.s = socket(AF_INET, SOCK_STREAM)
        self.leaderCheck()
        start_new_thread(self.startListening, ())
        start_new_thread(self.awaitInput, ())
        start_new_thread(self.startHeartbeat, ())

        while True:
            pass

    def receiveMessages(self, conn, addr):

        msg = conn.recv(1024).decode()
        print(msg)

        if "accepted " in msg:
            ballNum = int(msg.split()[1])
            v = int(msg.split()[-1])
            self.acceptances[self.accepts] = [ballNum, v]
            print("Acceptances: ")
            print(self.acceptances)
            self.accepts += 1
            if (self.accepts == 2):  # n-1
                print("Add to log")# Commit to log
                self.acceptances = [[0 for x in range(2)] for y in range(2)]
                self.accepts =0

        if "accept " in msg: #I am not a leader
            num = int(msg.split()[1])
            val = int(msg.split()[2])
            leaderID = msg.split()[-2]
            leaderport = int(msg.split()[-1])
            # print("My BallotNum when accept message received" + str(self.BallotNum.num))
            if(num>=self.BallotNum.num):
                self.AcceptNum.num = num
                self.AcceptNum.ID = leaderID
                self.AcceptVal = val # Accept Proposal
            message = "accepted "+ str(self.AcceptNum.num) + " "+ str(self.AcceptNum.ID) + " "+ str(self.AcceptVal) #####?????#####
            self.sendMessage(leaderport, message)

        if "heartbeat" in msg:
            self.leaderIsAlive = True

        if "Value received" in msg:
            valReceived = msg.split()[-2]
            if int(valReceived) > int(self.tickets):
                print("Only" + str(self.tickets) + "left")
            else:
                self.sendAcceptRequests(valReceived)
                self.tickets -= int(valReceived)


    def startHeartbeat(self):
        while True:
            time.sleep(1)
            self.sendToAll("heartbeat")




    def leaderCheck(self): #Election to be implemented
        # with open('config.json') as config:
        #     data = json.load(config)
        # config.close()
        # for i in configdata["kiosks"]:
        self.leaderport = 4003
        # if (configdata["leader"] == "False"):
        #     data["leader"] = "True"
        #     self.leaderport = self.port
        #     data["leaderID"] = str(self.port)
        #     with open('config.json', 'w') as config:
        #         json.dump(data, config)
        #     config.close()
        # else
        #     self.leaderport = configdata["leaderID"]
        #     print("Leader is " + self.leaderport)

    def sendAcceptRequests(self, val):
        initialValue = self.AcceptVal
        newVal = val
        self.BallotNum.num+=1
        message = "accept "+ str(self.BallotNum.num) + " "+ str(newVal) + " coming from "+ str(self.ID) + " " + str(self.port)
        self.sendToAll(message)



    def awaitInput(self):
        while True:
            message = input('Enter number of tickets you wish to buy ')
            val = int(message)
            try:
                    # tickets -= val
                  # change to if 'Buy 2' or 'show'
                    if (self.leaderport == self.port):
                        self.sendAcceptRequests(val)
                    else:
                        msg = "Value received " + str(val) + " " + str(self.port)
                        self.sendMessage(self.leaderport, msg)
            except ValueError:
                    print("Invalid Input")


    def startListening(self):
        # Add my details to configdata
        with open('live.json', 'a') as livefile:
            json.dump({ID:configdata["kiosks"][ID]}, livefile, ensure_ascii=False)
        # print(livefile)
        try:
            self.s.bind((self.hostname, int(self.port)))
            self.s.listen(3)
            print("server started on port " + str(self.port))
            while True:
                c, addr = self.s.accept()
                conn = c
                print('Got connection from')
                print(addr)
                start_new_thread(self.receiveMessages, (conn, addr))  # connection dictionary
        except(gaierror):
            print('There was an error making a connection')
            self.s.close()
            sys.exit()

    def sendMessage(self, port, message):
            rSocket = socket(AF_INET, SOCK_STREAM)
            rSocket.connect((gethostname(), int(port)))
            # print(message)
            rSocket.send(message.encode())
            rSocket.close()

    # To send messages to everyone
    def sendToAll(self, message):
        for i in configdata["kiosks"]:
            if (configdata["kiosks"][i][1] == self.port):  ## To not send to yourself
                continue
            else:
                try:
                    cSocket = socket(AF_INET, SOCK_STREAM)
                    ip, port = configdata["kiosks"][i][0], configdata["kiosks"][i][1]
                    port = int(port)
                    cSocket.connect((gethostname(), port))
                    print('Connected to port number ' + configdata["kiosks"][i][1])
                    cSocket.send(message.encode())
                    print('Message sent to customer at port ' + str(port))
                    cSocket.close()
                except ConnectionError:
                    print("Dead")

    def closeSocket(self):
        self.s.close()

######## MAIN #########

with open('config.json') as configfile:
    configdata = json.load(configfile)

delay = configdata["delay"]
ID = str(sys.argv[1])
tickets = configdata["tickets"]
c = Tickets(ID)
HEARTBEAT_FREQ = configdata["HEARTBEAT_FREQ"]
