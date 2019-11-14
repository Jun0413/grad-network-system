import argparse
import hashlib
import os
import xmlrpc.client


# read local index from <base_dir/index.txt>
def get_local_index(base_dir): # abs path
	local_index = {}
	index_file_path = os.path.join(base_dir, 'index.txt')
	if not os.path.exists(index_file_path):
		with open(index_file_path, 'w') as fp:
			pass
		return local_index
	
	with open(index_file_path, 'r') as fp:
		lines = fp.readlines()
		for line in lines:
			record = line.strip().split(' ', 2)
			local_index[record[0]] = [int(record[1]), record[2].split(' ')]

	return local_index


# add new files to local index
# update modified local index previously existed
# mark deleted local index to have 0 hashlist
def add_base_to_local(base_dir, local_index, block_size): # abs path
	base_local_index = {}
	for fn in os.listdir(base_dir):
		if fn == 'index.txt': # ignore index.txt
			continue
		hashlist = get_file_hashlist(os.path.join(base_dir, fn), block_size)
		if fn not in local_index: # new file
			base_local_index[fn] = [1, hashlist]
		else: # existing file
			if (hashlist == local_index[fn][1]): # not modified
				base_local_index[fn] = local_index[fn]
			else: # modified
				base_local_index[fn] = [local_index[fn][0] + 1, hashlist]
	for fn in local_index:
		if fn not in base_local_index: # deleted file
			base_local_index[fn] = [local_index[fn][0] + 1, ["0"]]
	return base_local_index


def get_remote_index(rpc):
	return rpc.surfstore.getfileinfomap()


# read a file in blocks specified by block_size
# return the hashlist
def get_file_hashlist(file_path, block_size): # abs path
	hashlist = []
	with open(file_path, 'rb') as fp:
		block = fp.read(block_size)
		while block:
			hashlist.append(hashlib.sha256(block).hexdigest())
			block = fp.read(block_size)
	return hashlist


# upload new blocks of a local file to server
def upload_file_blocks(rpc, base_dir, file_name, base_local_index, block_size):
	if base_local_index[file_name][1] == ["0"]:
		return
	file_path = os.path.join(base_dir, file_name)
	existed_hashes = set(rpc.surfstore.hasblocks(base_local_index[file_name][1]))
	with open(file_path, 'rb') as fp:
		hash_index = 0
		block = fp.read(block_size)
		while block:
			if base_local_index[file_name][1][hash_index] not in existed_hashes:
				rpc.surfstore.putblock(xmlrpc.client.Binary(block)) # TODO: directly upload?
			hash_index += 1
			block = fp.read(block_size)


# download a file on server
# since we don't update local index in this function
# therefore, we ignore deleted files
def download_file(rpc, base_dir, file_name, remote_index):
	file_path = os.path.join(base_dir, file_name)

	if remote_index[file_name][1] == ["0"]:
		if os.path.exists(file_path):
			os.remove(file_path)
		return
	
	with open(file_path, 'wb') as fp:
		for hash in remote_index[file_name][1]:
			fp.write(rpc.surfstore.getblock(hash).data)


# write updated_local_index to index.txt in <base_dir>
def commit_local_index(base_dir, updated_local_index):
	index_file_path = os.path.join(base_dir, 'index.txt')
	with open(index_file_path, 'w') as fp:
		for fn in updated_local_index:
			line = ' '.join([fn,\
			str(updated_local_index[fn][0]),\
			' '.join(updated_local_index[fn][1])])\
			+ '\n'
			fp.write(line)


# main function which client calls once after rpc setup
def sync(rpc, base_dir, block_size):
	local_index = get_local_index(base_dir)
	print('local index:')
	print(local_index, '\n')
	remote_index = get_remote_index(rpc)
	print('remote index received:')
	print(remote_index, '\n')
	base_local_index = add_base_to_local(base_dir, local_index, block_size)
	print('base index added to local index:')
	print(base_local_index, '\n')
	
	updated_local_index = {} # to commit to local index.txt afterwards
	local_remote_common_files = set() # to resolve conflicts later

	# I. upload new files to server
	for fn in base_local_index: # fn may be "deleted"
		if fn in remote_index:
			local_remote_common_files.add(fn)
		else: # upload file
			upload_file_blocks(rpc, base_dir, fn, base_local_index, block_size)
			if rpc.surfstore.updatefile(fn, base_local_index[fn][0], base_local_index[fn][1]):
				updated_local_index[fn] = base_local_index[fn]
				print('{} uploaded to server'.format(fn))
			else: # download file if upload failed (version not exactly greater by 1)
				remote_index = get_remote_index(rpc) # override outdated remote_index
				download_file(rpc, base_dir, fn, remote_index)
				updated_local_index[fn] = remote_index[fn]
				print('[warning] failed to upload {} with behind version, \
				download from server and override local'.format(fn))

	# II. download new files from server
	for fn in remote_index:
		if fn in base_local_index:
			local_remote_common_files.add(fn)
		else:
			download_file(rpc, base_dir, fn, remote_index)
			updated_local_index[fn] = remote_index[fn]
			print('{} downloaded from server'.format(fn))

	# III. handle conflicts on local_remote_common_files
	for fn in local_remote_common_files:
		local_version = base_local_index[fn][0]
		remote_version = remote_index[fn][0]
		download = False
		if local_version > remote_version:
			upload_file_blocks(rpc, base_dir, fn, base_local_index, block_size)
			if rpc.surfstore.updatefile(fn, local_version, base_local_index[fn][1]):
				updated_local_index[fn] = base_local_index[fn]
				print('{} uploaded to server'.format(fn))
			else:
				download = True
		elif local_version == remote_version:
			download = (base_local_index[fn][1] != remote_index[fn][1])
			updated_local_index[fn] = base_local_index[fn]
		else:
			download = True
		
		if download: # download file
			download_file(rpc, base_dir, fn, remote_index)
			updated_local_index[fn] = remote_index[fn]
			print('download {} from server and override local'.format(fn))

	# IV. commit updated_local_index to index.txt
	commit_local_index(base_dir, updated_local_index)


if __name__ == "__main__":

	parser = argparse.ArgumentParser(description="SurfStore client")
	parser.add_argument('hostport', help='host:port of the server')
	parser.add_argument('basedir', help='The base directory')
	parser.add_argument('blocksize', type=int, help='Block size')
	args = parser.parse_args()

	# TODO: validate command arguments

	try:
		client  = xmlrpc.client.ServerProxy('http://' + args.hostport)
		if client.surfstore.ping():
			print("Ping() successful")
		sync(client, args.basedir, args.blocksize)

	except Exception as e:
		print("Client: " + str(e))
