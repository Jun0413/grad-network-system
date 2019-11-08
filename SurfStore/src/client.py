import argparse
import xmlrpc.client

if __name__ == "__main__":

	parser = argparse.ArgumentParser(description="SurfStore client")
	parser.add_argument('hostport', help='host:port of the server')
	parser.add_argument('basedir', help='The base directory')
	parser.add_argument('blocksize', type=int, help='Block size')
	args = parser.parse_args()

	try:
		client  = xmlrpc.client.ServerProxy(args.hostport)
		# Test ping
		client.surfstore.ping()
		print("Ping() successful")

	except Exception as e:
		print("Client: " + str(e))
