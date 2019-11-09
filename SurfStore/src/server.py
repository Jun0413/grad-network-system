from socketserver import ThreadingMixIn
from threading import Lock
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler

import hashlib


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
        print(hash_to_block)
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
        if file_info_map[filename][0] >= version:
            return False
    file_info_map[filename] = [version, hashlist]
    return True


""" PROJECT 3 APIs below """

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
    print("Crash()")
    return True


# "Restores" this metadata store, allowing it to start responding
# to and sending RPCs to other nodes
def restore():
    """Restores this metadata store"""
    print("Restore()")
    return True


# "IsCrashed" returns the status of this metadata node (crashed or not)
# This method should always work, even when the node is crashed
def isCrashed():
    """Returns whether this node is crashed or not"""
    print("IsCrashed()")
    return True

if __name__ == "__main__":
    try:
        print("Attempting to start XML-RPC Server...")
        server = threadedXMLRPCServer(('localhost', 8080), requestHandler=RequestHandler)
        server.register_introspection_functions()
        server.register_function(ping,"surfstore.ping")
        server.register_function(getblock,"surfstore.getblock")
        server.register_function(putblock,"surfstore.putblock")
        server.register_function(hasblocks,"surfstore.hasblocks")
        server.register_function(getfileinfomap,"surfstore.getfileinfomap")
        server.register_function(updatefile,"surfstore.updatefile")

        server.register_function(isLeader,"surfstore.isleader")
        server.register_function(crash,"surfstore.crash")
        server.register_function(restore,"surfstore.restore")
        server.register_function(isCrashed,"surfstore.iscrashed")
        print("Started successfully.")
        print("Accepting requests. (Halt program to stop.)")
        server.serve_forever()
    except Exception as e:
        print("Server: " + str(e))
