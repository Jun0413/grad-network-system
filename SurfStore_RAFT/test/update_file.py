import argparse
import xmlrpc.client

if __name__ == "__main__":

	parser = argparse.ArgumentParser()
	parser.add_argument('hostport', help='host:port of the server')
	args = parser.parse_args()

	try:
		client  = xmlrpc.client.ServerProxy('http://' + args.hostport)
		if client.surfstore.updatefile("filename.txt", 1, ["hashlist"]):
			print("updateFile() successful")
			print(client.surfstore.getfileinfomap()['filename.txt'])

	except Exception as e:
		print("Client: " + str(e))
