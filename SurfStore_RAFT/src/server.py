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
    logging.debug("Receive GetFileInfoMap()")
    global status, file_info_map
    if status != 'Leader':
        raise Exception('Should only call getfileinfomap() on leader')
    if sendHeartbeatBlocked(5):
        return file_info_map
    else:
        return False # TODO: check here


def updateLocal(filename, version, hashlist):
    global file_info_map
    logging.debug("Update local file_info_map, filename = {0}, version = {1}".format(filename, version))
    if filename in file_info_map:
        if version - file_info_map[filename][0] != 1:
            return False
    file_info_map[filename] = [version, hashlist]
    return True


# Update a file's fileinfo entry, RPC call
def updatefile(filename, version, hashlist):
    logging.debug("Receive UpdateFile({0}, {1})".format(filename, version))
    global logs, status, commitIndex, lastApplied, currentTerm
    if status != 'Leader':
        raise Exception('Should only call updatefile() on leader')
    logs.append((currentTerm, filename, version, hashlist))
    # paper: respond after entry applied to state machine
    # project specification: if a majority of the nodes are working, should return the correct answer; 
    #         if a majority of the nodes are crashed, should block until a majority recover
    N = len(logs) - 1
    cnt = 1 # number of servers have logs update to logs[N]
    timeout = 5 # return false if timeout
    callTime = time.time()
    while time.time() - callTime < timeout:
        sendAppendEntries(False)
        cnt = 1
        for mid in matchIndex:
            if mid >= N:
                cnt += 1
        if cnt * 2 > len(serverlist) + 1 and logs[N][0] == currentTerm:
            break
        time.sleep(0.5)
    if cnt * 2 <= len(serverlist) + 1:
        # timeout
        logging.debug("UpdateFile() timeout")
        return False
    # commit to N
    result = True
    for i in range(lastApplied, N + 1):
        result = result and updateLocal(logs[i][1], logs[i][2], logs[i][3])
    commitIndex = N
    lastApplied = N
    sendAppendEntries(False) # ask clients to commit
    return result


# PROJECT 3 APIs below


# Queries whether this metadata store is a leader
# Note that this call should work even when the server is "crashed"
def isLeader():
    """Is this metadata store a leader?"""
    return status == 'Leader' and not crashed


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
def requestVote(term, serverid, lastLogIndex, lastLogTerm):
    """Requests vote to be the leader"""
    global votedFor, crashed, currentTerm
    logging.debug("Receive request vote from server {0}, server term = {1}, lastLogIndex = {2}, lastLogTerm = {3}".format(serverid, term, lastLogIndex, lastLogIndex))

    if crashed:
        raise Exception('Server crashed')  # TODO: is that right to indicate server crash?

    if term > currentTerm:
        setTerm(term)
        status = 'Follower'

    if votedFor != -1 or term < currentTerm or logs[-1][0] > lastLogTerm or (logs[-1][0] == lastLogTerm and len(logs) > lastLogIndex):
        # do we need to return currentTerm here?
        logging.debug("Refuse to vote")
        return False
    # grant vote to it
    votedFor = serverid
    logging.debug("Grant vote")
    return True


# Updates fileinfomap
def appendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit):
    """Updates fileinfomap to match that of the leader"""
    global startTime, status, crashed, logs, commitIndex, currentTerm
    if crashed:
        raise Exception('Server crashed')
    if term < currentTerm or status == 'Server':
        return False
    if term > currentTerm:
        setTerm(term)
        status = 'Follower'

    startTime = int((round(time.time() * 1000)))
    if prevLogIndex == -1:
        # heartbeat packet
        logging.debug("Received heartbeat appendEntries")
        status = 'Follower'
        setTerm(term)
        return True
    else:
        logging.debug("Received normal appendEntries from leader")
        if len(logs) <= prevLogIndex or logs[prevLogIndex][0] != prevLogTerm:
            return False
        entryId = 0
        for i in range(prevLogIndex + 1, len(logs)):
            if logs[i][0] != entries[entryId][0]:
                del logs[i:]
                break
            entryId += 1
        logs.extend(entries[entryId:])
        if leaderCommit > commitIndex:            
            N = min(leaderCommit, len(logs) - 1)
            result = True
            for i in range(lastApplied, N + 1):
                result = result and updateLocal(logs[i][1], logs[i][2], logs[i][3])
            commitIndex = N
            lastApplied = N
            return result
    return True


def tester_getversion(filename):
    return fileinfomap[filename][0]


def sendAppendEntriesSingle(prevLogIndex, index, result_list=[]):
    global currentTerm, servernum, commitIndex
    try:
        client = ServerProxy('http://' + serverlist[index])
        if prevLogIndex == -1:
            # heartbeat packet
            result = client.surfstore.appendEntries(currentTerm, servernum, prevLogIndex, 0, [], commitIndex)
            if result:
                result_list.append(index)
        else:
            # normal append
            result = client.surfstore.appendEntries(currentTerm, servernum, prevLogIndex, logs[prevLogIndex][0],
                                           logs[prevLogIndex + 1:], commitIndex)
            if result:
                # append successfully
                nextIndex[index] = len(logs)
                matchIndex[index] = len(logs) - 1
            else:
                nextIndex[index] -= 1
                sendAppendEntriesSingle(nextIndex[index], index) # retry until succeed  
    except Exception as e:
        logging.error("Client: " + str(e))


def sendHeartbeatBlocked(timeout=5):
    # block until timeout (False) or majority of servers reply true to the heartbeat msg
    callTime = time.time()
    while time.time() - callTime < timeout:
        result_list = [1]
        for i in range(0, len(serverlist)):
            x = threading.Thread(target=sendAppendEntriesSingle, args=(-1, i, result_list), daemon=True)
            x.start()
        time.sleep(1) # ugly implementation
        if len(result_list) * 2 > len(serverlist) + 1:
            return True
    return False


def sendAppendEntries(isHeartbeat):
    # prevLogIndex = -1, heartbeat packet
    global status, prevHeartbeatTime, serverlist, nextIndex
    if status != 'Leader':
        logging.error('Only leader can send appendEntries')
        return
    prevHeartbeatTime = int((round(time.time() * 1000)))
    for i in range(0, len(serverlist)):
        if isHeartbeat:
            prevLogIndex = -1
        else:
            prevLogIndex = nextIndex[i] - 1
        x = threading.Thread(target=sendAppendEntriesSingle, args=(prevLogIndex, i), daemon=True)
        x.start()


def becomeLeader():
    global status, nextIndex, matchIndex, startTime
    status = 'Leader'
    sendAppendEntries(True)
    startTime = int((round(time.time() * 1000)))
    # reinitialize variables
    nextIndex = [len(logs)] * len(serverlist)
    matchIndex = [0] * len(serverlist)

    

def setTerm(newTerm):
    global currentTerm, votedFor
    if newTerm < currentTerm:
        logging.error('NewTerm = {0}, smaller than currentTerm = {1}'.format(newTerm, currentTerm))
        return
    currentTerm = newTerm
    votedFor = -1


def askForVotes(hostport, index, votes):
    # ask server with hostport for vote
    global logs, currentTerm, servernum
    logging.debug("Ask for votes from server id = {0}, hostport = {1}".format(index, hostport))
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
        global status, time_factor, startTime, currentTerm, electionTimeout, serverlist, currentTerm, servernum, votedFor, votes, lock
        logging.debug('Start new election thread, status is ' + status)
        with lock:
            # update global status
            startTime = int((round(time.time() * 1000)))
            status = 'Candidate'
            electionTimeout = random.randint(400, 600) * time_factor
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
                return


# loop forever to monitor timeout
def main_loop():
    logging.debug('Monitor thread starts')
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
                sendAppendEntries(True)
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

        logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)  # TODO: change level to ERROR before submitting
        
        time_factor = 10 # 10 for debug, 1 for production
        # variables for RAFT protocol
        # persistent state on all servers
        currentTerm = 0  # latest term server has seen
        votedFor = -1  # candidateId that received vote in current term, -1 if none
        logs = []   # log entries, each entry contains cmd for state machine, and term when entry was received by leader
        # each entry is a tuple (term, filename, version, hashlist), index start from 1
        logs.append((1, '', '', []))

        # volatile state on all servers
        commitIndex = 0  # index of highest log entry known to be committed
        lastApplied = 0  # index of highest log entry applied to state machine
        status = 'Follower'  # 'Follower', 'Candidate', 'Leader'
        crashed = False  # is crashed

        # volatile state on leaders, reinitialize after election
        nextIndex = []  # for each server, index of the next log entry to send to that server, initialized to leader last log index + 1
        matchIndex = []  # for each server, index of highest log entry known to be replicated on server, intialized to 0

        # time
        random.seed(time.time())
        startTime = int((round(time.time() * 1000)))  # in milliseconds
        electionTimeout = random.randint(400, 600) * time_factor # in milliseconds TODO: choose appropriate time range
        heartbeatFreq = 250 * time_factor # in milliseconds
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
        monitor_thread = threading.Thread(target=main_loop, daemon=True)
        monitor_thread.start()
        server.serve_forever()

    except Exception as e:
        logging.error("Server: " + str(e))
