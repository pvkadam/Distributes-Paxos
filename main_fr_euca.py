from socket import *
from _thread import *
import json
import threading
import random
import time
import sys
import math
import ast

class BallotNum:
    def __init__(self, num, ID):
        self.num = num
        self.ID = ID

class Tickets:
    def __init__(self, ID):
        print("System running: " + ID)
        self.ID = ID #C1
        self.port = configdata["kiosks"][ID][1]
        self.processID = int(self.port) - 5000 #1
        self.hostname = gethostname()
        self.BallotNum = BallotNum(0,self.port)
        self.AcceptNum = BallotNum(0,0)
        self.AcceptVal = 0
        self.numOfAcks = 0
        self.accepts = 0
        self.pending = 0
        self.ticketsLeft = 1000
        self.majorityofLive = 2
        self.live = 3
        self.threadtimer = threading.Timer(3, self.setLeaderFalse)
        self.liveProcesses = [5001, 5002, 5003]
        self.leaderport = 0
        self.leaderIsAlive = False
        self.electionInProgress = False
        self.log = []
        w, h = 5, 2 # 4, n-1
        self.acks = [[0 for x in range(w)] for y in range(h)]
        self.acceptances = [[0 for x in range(4)] for y in range(10)] #n-1 rows
        self.s = socket(AF_INET, SOCK_STREAM)
        time.sleep(3)
        start_new_thread(self.startListening, ())
        start_new_thread(self.awaitInput, ())
        start_new_thread(self.startSendHeartbeat, ())

        while True:
           pass

    def receiveMessages(self, conn, addr):
        msg = conn.recv(1024).decode()

        if "Leader" in msg:
            print(msg)
            self.leaderport = int(msg.split()[-1])
            self.electionInProgress = False
            self.leaderIsAlive = True
            mesg = "Election ended"
            print(mesg)
            # self.sendToAll(mesg)

        if "prepare" in msg:
            print(msg)
            num1 = int(msg.split()[1])
            sendToPort = int(msg.split()[-1])
            if num1 > self.BallotNum.num or (num1 == self.BallotNum.num and sendToPort > int(self.BallotNum.ID)):
                self.BallotNum.num = num1
                message = "ack " + str(self.BallotNum.num) + " " + str(self.AcceptNum.num) + " " + str(self.AcceptNum.ID) +" " + str(self.AcceptVal)
                self.sendMessage(sendToPort, message)

        if "ack" in msg:
            print(msg)
            num1 = int(msg.split()[1])
            receivedVal = int(msg.split()[-1])
            self.numOfAcks += 1
            # time.sleep(1)
            if receivedVal > self.AcceptVal:
                self.AcceptVal = receivedVal
            if self.numOfAcks == self.majorityofLive: # needs to be majority of live processes
                self.leaderport = self.port
                msg = "Leader " + str(self.leaderport)
                start_new_thread(self.startSendHeartbeat, ())
                      # str(self.BallotNum.num) + " " + str(self.AcceptVal)
                self.sendToAll(msg)
                if self.pending > 0:
                    self.sendAcceptRequests(self.pending)

        if "accepted " in msg:
            print(msg)
            ballNum = int(msg.split()[1])
            receivedID = msg.split()[2]
            v = int(msg.split()[-1])
            self.acceptances[self.accepts] = [ballNum, receivedID, v]
            # time.sleep(3)
            # print("Acceptances: ")
            # print(self.acceptances)
            self.accepts += 1
            if (self.accepts == self.majorityofLive):  # n-1
                message = "Add to log " + str(v) # Commit to log
                self.ticketsLeft = self.ticketsLeft - v
                print(message)
                self.log.append("Buy "+ str(v))
                self.sendToAll(message)
                self.acceptances = [[0 for x in range(4)] for y in range(10)]
                self.accepts =0

        if "accept " in msg: #I am not a leader
            print(msg)
            num = int(msg.split()[1])
            val = int(msg.split()[2])
            senderID = msg.split()[-2]
            senderport = int(msg.split()[-1])
            ticketsAfterSale = self.ticketsLeft - val
            time.sleep(1)
            if ticketsAfterSale >= 0:
                if num > self.BallotNum.num or (num == self.BallotNum.num and senderport > int(self.BallotNum.ID)):
                    self.AcceptNum.num = num
                    self.AcceptNum.ID = senderID
                    self.AcceptVal = val # Accept Proposal
                message = "accepted "+ str(self.AcceptNum.num) + " "+ str(self.AcceptNum.ID) + " "+ str(self.AcceptVal)
                self.sendMessage(senderport, message)
            else:
                print("Not enough tickets for your order.")


        if "Value received" in msg: #By leader
            print(msg)
            valReceived = msg.split()[-2]
            self.sendAcceptRequests(valReceived)

        if "heartbeat" in msg: # leader's log received and made my log
            # print(msg)
            msg = msg.replace("heartbeat ", '')
            leader = msg.split()[0]
            self.leaderport = int(leader)
            self.ticketsLeft = int(msg.split()[1])
            msg = msg.split(' ', 2)[2]
            self.log = ast.literal_eval(msg)
            self.threadtimer.cancel()
            self.leaderIsAlive = True
            self.startTimer()

        if "Add to log" in msg:
            print(msg)
            if "failed" in msg or "added" in msg:
                self.log.append(msg.split()[-2] + " " + msg.split()[-1])
            else:
                val = int(msg.split()[-1])
                self.log.append("Buy " + str(val))
                self.ticketsLeft = self.ticketsLeft - val
                print("Current log is: ")
                print(self.log)
                print("Tickets Left: " + str(self.ticketsLeft))

        if "Live" in msg:
            self.live = int(msg.split()[-1])

        if "Processes" in msg:
            msg = msg.replace("Processes ", '')
            self.liveProcesses = ast.literal_eval(msg)


    def leaderCheck(self): #Check if Leader is alive
        # time.sleep(0.25)
        if self.leaderIsAlive == False:
            if self.electionInProgress == False:
                self.startElection()
        elif self.leaderport == int(self.port):
            self.startSendHeartbeat()

    def startElection(self): # Leader down, new election begun
        m = "Election begun"
        print(self.leaderIsAlive)
        self.electionInProgress =True
        print(m)
        self.sendToAll(m)
        time.sleep(3)
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
            except ValueError:
                print("Invalid Input")

            if "Buy" in message:
                time.sleep(0.25)
                val = int(message.split()[-1])
                self.pending = val

                if (self.leaderport == self.port):
                    self.sendAcceptRequests(val)
                    self.pending =0
                elif self.leaderIsAlive == True :
                    msg = "Value received " + str(val) + " " + str(self.port)
                    self.sendMessage(self.leaderport, msg)
                else:
                    self.leaderCheck()

            if "show" in message:
                self.sendToAll('')
                print("Last committed state to log is: ")
                print(self.log[-1])
                print("My log is: ")
                print(self.log)
                self.leaderCheck()


    def startListening(self):
        # Add my details to configdata
        try:
            self.s.bind((self.hostname, int(self.port)))
            self.s.listen(5)
            print("server started on port " + str(self.port))
            while True:
                c, addr = self.s.accept()
                conn = c
                # print('Got connection from')
                # print(addr)
                start_new_thread(self.receiveMessages, (conn, addr))
        except(gaierror):
            print('There was an error making a connection')
            self.s.close()
            sys.exit()

    def returnNotMatches(self, a, b):
        c = [[x for x in a if x not in b], [x for x in b if x not in a]]
        d = [x for x in c if x != []]
        return d[0][0]

    def sendMessage(self, port, message):
        rSocket = socket(AF_INET, SOCK_STREAM)
        iptoSend = ""
        for i in configdata["kiosks"]:
            ip, portfromlist = configdata["kiosks"][i][0], configdata["kiosks"][i][1]
            if int(portfromlist) == port:
                iptoSend = ip
        rSocket.connect((iptoSend, int(port)))
#         rSocket.connect((gethostname(), int(port)))
        rSocket.send(message.encode())
        rSocket.close()

    def startTimer(self):
        self.threadtimer = threading.Timer(3, self.setLeaderFalse)
        self.threadtimer.start()

    def setLeaderFalse(self):
        self.leaderIsAlive = False

    def startSendHeartbeat(self):
        while True:
            if self.leaderport == self.port:
                while True:
                    time.sleep(0.5)
                    self.sendHeartbeat()

    def sendHeartbeat(self): #send entire log instead of text
        msg = "heartbeat "
        msg= msg + str(self.port) + " "+ str(self.ticketsLeft) + " "+  str(self.log)
        self.sendToAll(msg)

    # To send messages to everyone
    def sendToAll(self, message):
        newliveProcesses = []
        for i in configdata["kiosks"]:
            if (configdata["kiosks"][i][1] == self.port):  ## To not send to yourself
                continue
            else:
                try:
                    cSocket = socket(AF_INET, SOCK_STREAM)
                    ip, port = configdata["kiosks"][i][0], configdata["kiosks"][i][1]
                    # portnum = port
                    port = int(port)
                    cSocket.connect((ip, port))
                    # print('Connected to port number ' + configdata["kiosks"][i][1])
                    cSocket.send(message.encode())
                    # print('Message sent to customer at port ' + str(port))
                    newliveProcesses.append(port)
                    # numofLive += 1
                    cSocket.close()
                except ConnectionError:
                    pass
        newliveProcesses.append(int(self.port))
        numofLive = len(newliveProcesses)
        if numofLive == 1:
            sys.exit()
        # print("Number of live processes " + str(numofLive))
        self.majorityofLive = math.ceil(numofLive/2)

        if numofLive > self.live or numofLive < self.live:             # or newliveProcesses!=self.liveProcesses:
            self.configChanges(numofLive, newliveProcesses)

    def configChanges(self, numofLive, newliveProcesses):
        if numofLive > self.live:
            c= self.returnNotMatches(newliveProcesses, self.liveProcesses)
            self.liveProcesses = newliveProcesses
            self.live = numofLive
            self.sendToAll("Live " + str(self.live))
            self.sendToAll("Processes " + str(self.liveProcesses))
            message = "Add to log " + str(c) + " added"
            print(message)
            self.sendToAll(message)
            self.log.append(message.split()[-2] + " " + message.split()[-1])


        if numofLive < self.live:
            c =self.returnNotMatches(newliveProcesses, self.liveProcesses)
            self.liveProcesses = newliveProcesses
            self.live = numofLive
            self.sendToAll("Live " + str(self.live))
            self.sendToAll("Processes " + str(self.liveProcesses))
            message = "Add to log " + str(c) + " failed"
            print(message)
            self.sendToAll(message)
            self.log.append(message.split()[-2] + " " + message.split()[-1])


    def closeSocket(self):
        self.s.close()

######## MAIN #########

with open('config.json') as configfile:
    configdata = json.load(configfile)

delay = configdata["delay"]
ID = str(sys.argv[1])
tickets = configdata["tickets"]
c = Tickets(ID)
