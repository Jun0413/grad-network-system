import argparse
import xmlrpc.client

if __name__ == "__main__":

	parser = argparse.ArgumentParser()
	parser.add_argument('hostport', help='host:port of the server')
	args = parser.parse_args()

	try:
		client  = xmlrpc.client.ServerProxy('http://' + args.hostport)
		print(client.surfstore.getfileinfomap())

	except Exception as e:
		print("Client: " + str(e))
