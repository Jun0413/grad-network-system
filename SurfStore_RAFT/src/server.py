from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from xmlrpc.client import ServerProxy
import hashlib
import argparse
import time
import random
import threading
import logging

""" Threaded XML-RPC Server """
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)


class threadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass




""" In-Memory Storage """
# map hash[str] to block[bytes]
hash_to_block = {}
# map file_name[str] to info[list]
# in the info, 1st element is version[int]
# second element is hashes[ordered list] 
file_info_map = {}


# A simple ping, returns true
def ping():
    print("Ping()")
    return True


# Gets a block, given a specific hash value
def getblock(h):
    print("GetBlock(" + h + ")")
    if h not in hash_to_block:
        return None # should not happen
    print(type(hash_to_block))
    return hash_to_block[h]


# Puts a block
# TODO: (1) decide if b is bytes (2) return True?
def putblock(b):
    print("PutBlock()")
    global hash_to_block
    hash_to_block[hashlib.sha256(b.data).hexdigest()] = b.data
    return True


# Given a list of blocks, return the subset that are on this server
# TODO:
# (1) originally was "blocklist", changed to "hashlist"
# (2) need to check if it is deleted?
def hasblocks(hashlist):
    print("HasBlocks()")
    return [hash for hash in hashlist if hash in hash_to_block]


# Retrieves the server's FileInfoMap
def getfileinfomap():
    print("GetFileInfoMap()")
    return file_info_map


# Update a file's fileinfo entry
def updatefile(filename, version, hashlist):
    print("UpdateFile()")
    global file_info_map
    if filename in file_info_map:
        if version - file_info_map[filename][0] != 1:
            return False
    file_info_map[filename] = [version, hashlist]
    return True


# PROJECT 3 APIs below


# Queries whether this metadata store is a leader
# Note that this call should work even when the server is "crashed"
def isLeader():
    """Is this metadata store a leader?"""
    print("IsLeader()")
    return True


# "Crashes" this metadata store
# Until Restore() is called, the server should reply to all RPCs
# with an error (unless indicated otherwise), and shouldn't send
# RPCs to other servers
def crash():
    """Crashes this metadata store"""
    global crashed
    crashed = True
    return True


# "Restores" this metadata store, allowing it to start responding
# to and sending RPCs to other nodes
def restore():
    """Restores this metadata store"""
    global crashed
    crashed = False
    return True


# "IsCrashed" returns the status of this metadata node (crashed or not)
# This method should always work, even when the node is crashed
def isCrashed():
    """Returns whether this node is crashed or not"""
    return crashed


# Requests vote from this server to become the leader
def requestVote(serverid, term, lastLogIndex, lastLogTerm):
    """Requests vote to be the leader"""
    global votedFor, crashed
    if crashed:
        raise Exception('Server crashed')  # TODO: is that right to indicate server crash?
    if votedFor != -1 or term < currentTerm or logs[-1][0] > lastLogTerm or (logs[-1][0] == lastLogTerm and len(logs) > lastLogIndex):
        # do we need to return currentTerm here?
        return False
    # grant vote to it
    votedFor = serverid
    return True


# Updates fileinfomap
def appendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit):
    """Updates fileinfomap to match that of the leader"""
    global startTime, status, crashed
    if crashed:
        raise Exception('Server crashed')
    startTime = int((round(time.time() * 1000)))
    if prevLogIndex == -1:
        # heartbeat packet
        logging.debug("Received heartbeat appendEntries")
        if term >= currentTerm:
            status = 'Follower'
            setTerm(term)
    else:
        logging.debug("Received normal appendEntries from leader")
        # TODO
    return True


def tester_getversion(filename):
    return fileinfomap[filename][0]


def sendAppendEntriesSingle(prevLogIndex, hostport):
    global currentTerm, servernum, commitIndex
    try:
        client = ServerProxy('http://' + hostport)
        if prevLogIndex == -1:
            # heartbeat packet
            client.surfstore.appendEntries(currentTerm, servernum, prevLogIndex, 0, [], commitIndex)
        else:
            # normal append
            client.surfstore.appendEntries(currentTerm, servernum, prevLogIndex, logs[prevLogIndex][0],
                                           logs[prevLogIndex + 1:], commitIndex)

        # TODO: handle return value
    except Exception as e:
        print("Client: " + str(e))


def sendAppendEntries(prevLogIndex):
    # prevLogIndex = -1, heartbeat packet
    global status, prevHeartbeatTime, serverlist
    if status != 'Leader':
        logging.error('Only leader can send appendEntries')
        return
    prevHeartbeatTime = int((round(time.time() * 1000)))
    for addr in serverlist:
        x = threading.Thread(target=sendAppendEntriesSingle, args=(prevLogIndex, addr), daemon=True)
        x.start()


def becomeLeader():
    global status, nextIndex, matchIndex, startTime
    status = 'Leader'
    sendAppendEntries(-1)
    startTime = int((round(time.time() * 1000)))
    # reinitialize variables
    nextIndex = [len(logs)] * len(serverlist)
    matchIndex = [0] * len(serverlist)


def setTerm(newTerm):
    global currentTerm, votedFor
    if newTerm <= currentTerm:
        logging.error('NewTerm should >= currentTerm')
        return
    currentTerm = newTerm
    votedFor = -1


def askForVotes(hostport, index, votes):
    # ask server with hostport for vote
    global logs, currentTerm, servernum
    try:
        client = ServerProxy('http://' + hostport)
        if client.surfstore.requestVote(currentTerm, servernum, len(logs), logs[-1][0]):
            votes.append(index)  # list append is thread safe, no need acquire lock
    except Exception as e:
        logging.error("Client: " + str(e))


class electionThread(threading.Thread):
    # The thread should check stopped() condition regularly
    def __init__(self,  *args, **kwargs):
        super(electionThread, self).__init__(*args, **kwargs)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def stopped(self):
        return self.stop_event.is_set()

    def run(self):
        global status, startTime, currentTerm, electionTimeout, serverlist, currentTerm, servernum, votedFor, votes, lock
        logging.debug('Start new election thread, status is ' + status)
        with lock:
            # update global status
            startTime = int((round(time.time() * 1000)))
            status = 'Candidate'
            electionTimeout = random.randint(400, 600)
            term = currentTerm + 1
            setTerm(term)
            votedFor = servernum
        votes = [-1] # vote for itself
        # request vote from every other servers
        for i in range(0, len(serverlist)):
            x = threading.Thread(target=askForVotes, args=(serverlist[i], i, votes), daemon=True)
            x.start()
        # monitor votes
        while True:
            if status != 'Candidate' or currentTerm > term or self.stopped():
                # other server may become leader in the process
                logging.debug('Failed to become leader, exit leader election')
                return
            if len(votes) * 2 > len(serverlist) + 1:
                # TODO: test even condition?
                # become leader
                logging.debug('Successfully elected to be the leader')
                becomeLeader()


# loop forever to monitor timeout
def main_loop():
    electionTask = None
    while True:
        if not crashed:
            curTime = int((round(time.time() * 1000)))
            if status != 'Leader' and curTime - startTime > electionTimeout:
                logging.debug('Election timeout')
                # elect new leader
                if electionTask is not None and electionTask.is_alive():
                    logging.debug('Previous leader election does not complete, stop it')
                    electionTask.stop()
                electionTask = electionThread(daemon=True)
                electionTask.start()
            if status == 'Leader' and curTime - prevHeartbeatTime > heartbeatFreq:
                logging.debug('Send heartbeat packet')
                sendAppendEntries(-1)
        time.sleep(0.05)


# Reads the config file and return host, port and store list of other servers
def readconfig(config, servernum):
    """Reads cofig file"""
    fd = open(config, 'r')
    l = fd.readline()

    maxnum = int(l.strip().split(' ')[1])

    if servernum >= maxnum or servernum < 0:
        raise Exception('Server number out of range.')

    d = fd.read()
    d = d.splitlines()

    for i in range(len(d)):
        hostport = d[i].strip().split(' ')[1]
        if i == servernum:
            host = hostport.split(':')[0]
            port = int(hostport.split(':')[1])

        else:
            serverlist.append(hostport)

    return maxnum, host, port


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description="SurfStore server")
        parser.add_argument('config', help='path to config file')
        parser.add_argument('servernum', type=int, help='server number')

        args = parser.parse_args()

        config = args.config
        servernum = args.servernum

        # server list has list of other servers
        serverlist = []

        # maxnum is maximum number of servers
        maxnum, host, port = readconfig(config, servernum)

        hashmap = dict()

        fileinfomap = dict()

        logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)  # TODO: change level to ERROR before submitting
        # variables for RAFT protocol
        # persistent state on all servers
        currentTerm = 0  # latest term server has seen
        votedFor = -1  # candidateId that received vote in current term, -1 if none
        logs = []   # log entries, each entry contains cmd for state machine, and term when entry was received by leader
        # each entry is a tuple (term, cmd, args), index start from 1
        logs.append((1, '', ['']))

        # volatile state on all servers
        commitIndex = 0  # index of highest log entry known to be committed
        lastApplied = 0  # index of highest log entry applied to state machine
        status = 'Follower'  # 0 follower, 1 candidate, 2 leader
        crashed = False  # is crashed

        # volatile state on leaders, reinitialize after election
        nextIndex = []  # for each server, index of the next log entry to send to that server, initialized to leader last log index + 1
        matchIndex = []  # for each server, index of highest log entry known to be replicated on server, intialized to 0

        # time
        random.seed(time.time())
        startTime = int((round(time.time() * 1000)))  # in milliseconds
        electionTimeout = random.randint(400, 600)  # in milliseconds TODO: choose appropriate time range
        heartbeatFreq = 250  # in milliseconds
        prevHeartbeatTime = 0
        logging.debug('Server {0} start'.format(servernum))

        lock = threading.Lock()

        logging.debug("Attempting to start XML-RPC Server...")
        server = threadedXMLRPCServer((host, port), requestHandler=RequestHandler)
        server.register_introspection_functions()
        server.register_function(ping,"surfstore.ping")
        server.register_function(getblock,"surfstore.getblock")
        server.register_function(putblock,"surfstore.putblock")
        server.register_function(hasblocks,"surfstore.hasblocks")
        server.register_function(getfileinfomap,"surfstore.getfileinfomap")
        server.register_function(updatefile,"surfstore.updatefile")
        # Project 3 APIs
        server.register_function(isLeader,"surfstore.isLeader")
        server.register_function(crash,"surfstore.crash")
        server.register_function(restore,"surfstore.restore")
        server.register_function(isCrashed,"surfstore.isCrashed")
        server.register_function(requestVote,"surfstore.requestVote")
        server.register_function(appendEntries,"surfstore.appendEntries")
        server.register_function(tester_getversion,"surfstore.tester_getversion")
        logging.debug("Started successfully.")
        logging.debug("Accepting requests. (Halt program to stop.)")
        server.serve_forever()
        main_loop()

    except Exception as e:
        logging.error("Server: " + str(e))
