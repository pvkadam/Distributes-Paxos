from socket import *
from _thread import *
import json
import threading
import random
import time
import sys
import math

class BallotNum:
    def __init__(self, num, ID):
        self.num = num
        self.ID = ID

class Tickets:
    def __init__(self, ID):
        print("System running: " + ID)
        self.ID = ID #C1
        self.port = configdata["kiosks"][ID][1]
        self.processID = int(self.port) - 4000 #1
        self.hostname = gethostname()
        self.BallotNum = BallotNum(0,self.port)
        self.AcceptNum = BallotNum(0,0)
        self.AcceptVal = 0
        self.numOfAcks = 0
        self.accepts = 0
        self.pending = 0
        self.majorityofLive = 2
        self.leaderport = 0
        self.leaderIsAlive = False
        self.electionInProgress = False
        self.log = []
        w, h = 5, 2 # 4, n-1
        self.acks = [[0 for x in range(w)] for y in range(h)]
        self.acceptances = [[0 for x in range(4)] for y in range(4)] #n-1 rows
        self.s = socket(AF_INET, SOCK_STREAM)
        time.sleep(5)
        # self.leaderCheck()
        start_new_thread(self.startListening, ())
        start_new_thread(self.awaitInput, ()) # If leader send Heartbeat else timer
        start_new_thread(self.startSendHeartbeat, ())

        while True:
           pass

    def receiveMessages(self, conn, addr):
        msg = conn.recv(1024).decode()
        print(msg)

        if "Leader" in msg:
            self.leaderport = int(msg.split()[-1])
            self.electionInProgress = False
            self.leaderIsAlive = True
            mesg = "Election ended"
            print(mesg)
            # self.sendToAll(mesg)

        if "prepare" in msg:
            num1 = int(msg.split()[1])
            sendToPort = int(msg.split()[-1])
            if num1 > self.BallotNum.num or (num1 == self.BallotNum.num and sendToPort > int(self.BallotNum.ID)):
                self.BallotNum.num = num1
                message = "ack " + str(self.BallotNum.num) + " " + str(self.AcceptNum.num) + " " + str(self.AcceptNum.ID) +" " + str(self.AcceptVal)
                self.sendMessage(sendToPort, message)

        if "ack" in msg:
            num1 = int(msg.split()[1])
            receivedVal = int(msg.split()[-1])
            self.numOfAcks += 1
            # majority = math.ceil((len(CLIENTS) + 2) / 2)
            if receivedVal > self.AcceptVal:
                self.AcceptVal = receivedVal
            if self.numOfAcks == self.majorityofLive: # needs to be majority of live processes
                self.leaderport = self.port
                msg = "Leader " + str(self.leaderport)
                start_new_thread(self.startSendHeartbeat, ())
                      # str(self.BallotNum.num) + " " + str(self.AcceptVal)
                self.sendToAll(msg)
                self.sendAcceptRequests(self.pending)

        if "accepted " in msg:
            ballNum = int(msg.split()[1])
            receivedID = msg.split()[2]
            v = int(msg.split()[-1])
            self.acceptances[self.accepts] = [ballNum, receivedID, v]
            print("Acceptances: ")
            print(self.acceptances)
            self.accepts += 1
            if (self.accepts == self.majorityofLive):  # n-1
                message = "Add to log " + str(v) # Commit to log
                print(message)
                self.log.append(v)
                self.sendToAll(message)
                self.acceptances = [[0 for x in range(4)] for y in range(4)]
                self.accepts =0

        if "accept " in msg: #I am not a leader
            num = int(msg.split()[1])
            val = int(msg.split()[2])
            senderID = msg.split()[-2]
            senderport = int(msg.split()[-1])
            # print("My BallotNum when accept message received" + str(self.BallotNum.num))
            if num > self.BallotNum.num or (num == self.BallotNum.num and senderport > int(self.BallotNum.ID)):
                self.AcceptNum.num = num
                self.AcceptNum.ID = senderID
                self.AcceptVal = val # Accept Proposal
            message = "accepted "+ str(self.AcceptNum.num) + " "+ str(self.AcceptNum.ID) + " "+ str(self.AcceptVal) #####?????#####
            self.sendMessage(senderport, message)

        if "Value received" in msg: #By leader
            valReceived = msg.split()[-2]
            self.sendAcceptRequests(valReceived)

        if "heartbeat" in msg: # leader's log received and made my log
            msg = msg.replace("heartbeat ", '')
            msg = json.loads(msg)
            self.log = msg
            self.leaderIsAlive = True
            self.timer()
            self.leaderIsAlive = False

        if "Add to log" in msg:
            val = int(msg.split()[-1])
            self.log.append(val)
            print("Current log is: ")
            print(self.log)

    def leaderCheck(self): #Check if Leader is alive
        if self.leaderIsAlive == False:
            if self.electionInProgress == False:
                self.startElection()
        elif self.leaderport == int(self.port):
            self.startSendHeartbeat()

    def startElection(self): # Leader down, new election begun
        message = "Election begun"
        self.electionInProgress =True
        self.sendToAll(message)
        time.sleep(5)
        self.BallotNum.num += 1
        message = "prepare " + str(self.BallotNum.num) + " " + str(self.BallotNum.ID)
        self.sendToAll(message)

    def sendAcceptRequests(self, val):
        initialValue = self.AcceptVal
        newVal = val
        self.BallotNum.num+=1
        message = "accept "+ str(self.BallotNum.num) + " "+ str(newVal) + " coming from "+ str(self.ID) + " " + str(self.port)
        self.sendToAll(message)

    def awaitInput(self):
        while True:
            try:
                message = input('Hello, welcome to the Ticket Kiosk.')
                if "Buy" in message:
                        val = int(message.split()[-1])
                        self.pending = val

                        if (self.leaderport == self.port):
                            self.sendAcceptRequests(val)
                        elif self.leaderIsAlive == True :
                            msg = "Value received " + str(val) + " " + str(self.port)
                            self.sendMessage(self.leaderport, msg)
                        else:
                            self.leaderCheck()

                if "show" in message:
                        print("My log is: ")
                        print(self.log)

            except ValueError:
                print("Invalid Input")
            #
            # if "Leader" in message:
            #     msg = "Leader " + self.port
            #     self.sendToAll(msg)


    def startListening(self):
        # Add my details to configdata
        with open('live.json', 'a') as livefile:
            json.dump({ID:configdata["kiosks"][ID]}, livefile, ensure_ascii=False)
        # print(livefile)
        try:
            self.s.bind((self.hostname, int(self.port)))
            self.s.listen(5)
            print("server started on port " + str(self.port))
            while True:
                c, addr = self.s.accept()
                conn = c
                # print('Got connection from')
                # print(addr)
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

    def timer(self):
        timetosleep = 0.4 + (self.processID*0.1)
        time.sleep(timetosleep) # Sleep for 0.4s + processID*0.1

    def startSendHeartbeat(self):
        # self.t = threading.Timer(1.0, self.sendHeartbeat)
        # self.t.start()
        while True:
            if self.leaderport == self.port:
                while True:
                    time.sleep(1)
                    self.sendHeartbeat()

    # def stopSendHeartbeat(self):
    #     self.t.cancel()

    def sendHeartbeat(self): #send entire log instead of text
        msg = "heartbeat "
        msg= msg + str(self.log)
        self.sendToAll(msg)

    # To send messages to everyone
    def sendToAll(self, message):
        # portnum = 0
        numofLive = 0
        for i in configdata["kiosks"]:
            if (configdata["kiosks"][i][1] == self.port):  ## To not send to yourself
                continue
            else:
                try:
                    cSocket = socket(AF_INET, SOCK_STREAM)
                    ip, port = configdata["kiosks"][i][0], configdata["kiosks"][i][1]
                    # portnum = port
                    port = int(port)
                    cSocket.connect((gethostname(), port))
                    print('Connected to port number ' + configdata["kiosks"][i][1])
                    cSocket.send(message.encode())
                    print('Message sent to customer at port ' + str(port))
                    numofLive += 1
                    cSocket.close()
                except ConnectionError:
                    pass
        numofLive += 1
        if numofLive == 1:
            sys.exit()
        print("Number of live processes " + str(numofLive))
        self.majorityofLive = math.ceil(numofLive/2)
                    # print(str(portnum) + " is dead")

    def closeSocket(self):
        self.s.close()

######## MAIN #########

with open('config.json') as configfile:
    configdata = json.load(configfile)

delay = configdata["delay"]
ID = str(sys.argv[1])
tickets = configdata["tickets"]
c = Tickets(ID)


