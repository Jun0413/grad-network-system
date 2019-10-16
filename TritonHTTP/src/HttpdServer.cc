#include <sysexits.h>


#include "logger.hpp"
#include "HttpdServer.hpp"


HttpdServer::HttpdServer(INIReader& t_config)
	: config(t_config) {
	auto log = logger();

	std::string pstr = config.Get("httpd", "port", "");
	if (pstr == "") {
		log->error("port was not in the config file");
		exit(EX_CONFIG);
	}
	port = pstr;

	std::string dr = config.Get("httpd", "doc_root", "");
	if (dr == "") {
		log->error("doc_root was not in the config file");
		exit(EX_CONFIG);
	}
	doc_root = dr;
}


// start TCP server and create a thread for each connection
void HttpdServer::launch() {
	auto log = logger();

	log->info("Launching web server");
	log->info("Port: {}", port);
	log->info("doc_root: {}", doc_root);

	// TODO: where to put these variables
	int serv_sock, clnt_sock;
	struct addrinfo hints, *servinfo, *p;
	int rv;
	struct sockaddr_in clnt_addr;
	unsigned int clnt_addr_len;
	int MAX_NUM_CLNTS = 5;
	int sock_opt = 1;

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;

	if ((rv = getaddrinfo(NULL, port.c_str(), &hints, &servinfo)) != 0) {
		log->error("getaddrinfo: {}", gai_strerror(rv));
		exit(1);
	}

	for (p = servinfo; p != NULL; p = p->ai_next) {
		if ((serv_sock = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
			log->error("socket(), try next addr");
			continue;
		}
		if (bind(serv_sock, p->ai_addr, p->ai_addrlen) == -1) {
			close(serv_sock);
			log->error("bind(), try next addr");
			continue;
		}
		break;
	}

	if (p == NULL) {
		log->error("no addr available to create and bind socket");
		exit(1);
	}

	freeaddrinfo(servinfo);

	if (listen(serv_sock, MAX_NUM_CLNTS) < 0) {
		log->error("listen()");
		exit(1);
	}

	if (setsockopt(serv_sock, SOL_SOCKET, SO_REUSEADDR, &sock_opt, sizeof(sock_opt)) != 0) {
		log->error("setsockopt() for server socket");
		exit(1);
	}

	while (true) {

        clnt_addr_len = sizeof(clnt_addr);
        if ((clnt_sock = accept(serv_sock,
		    (struct sockaddr *) &clnt_addr, &clnt_addr_len)) < 0) {
			log->error("accept()");
			continue;
		}

		log->info("accept client {}", inet_ntoa(clnt_addr.sin_addr));

		// TODO: set timeout
		struct timeval timeout;
		timeout.tv_sec = 5;
		timeout.tv_usec = 0;
		if (setsockopt(clnt_sock, SOL_SOCKET, SO_RCVTIMEO,
		    (char *)&timeout, sizeof(timeout)) < 0) {
			log->error("setsockopt() for client socket");
			exit(1);
		}

		// create a new thread to handle a client
		std::thread t(&HttpdServer::handle_client, this, clnt_sock);
		t.detach();
    }

	close(serv_sock);
}


// independent thread to handle a client
void HttpdServer::handle_client(int clnt_sock) {
	auto log = logger();
	log->info("handle client socket: {}", clnt_sock);

	// where to put this param?
	const int MAX_RECV_BUF_SIZE = 4096;

	while (true) {
		std::string req_str;
		int bytes_recv = 0;
		// 1. get request string (close if timeout)
		while (true) {
			std::vector<char> buffer(MAX_RECV_BUF_SIZE);
			bytes_recv = recv(clnt_sock, &buffer[0], buffer.size(), 0);

			if (bytes_recv < 0) {
				// check if it is timed out
				if (errno == EAGAIN || errno == EWOULDBLOCK) {
					log->info("receive time out");
					// TODO
					// close socket, send back a connection:close
					// and stop handling this client
					close(clnt_sock);
					return;
				}
				log->error("recv()");
				//TODO
				// close socket, (send error code?)
				// and stop handling this client
				close(clnt_sock);
				return;
			}

			if (bytes_recv != 0) {
				req_str.append(buffer.cbegin(), buffer.cend());
			}

			if (bytes_recv < MAX_RECV_BUF_SIZE) {
				break;
			}
		}

		log->info("receive: {}", req_str);

		// 2. validate and parse request string (possibly pipelined)
		std::vector<string> urls = parse_request(req_str);

		// 3. locate files (mime types, security, ...)
		// and send files
		for (string url : urls) {
			log->info("retrieving file {}", url);
			if (is_path_accessible(url)) {
				char* abs_path = realpath(url.c_str(), NULL);
				log->info("absolute path: {}", abs_path);
				int fd = open(abs_path, O_RDONLY);
				if (fd == -1) {
					log->error("open() file");
					continue;
				}
				// sendfile(clnt_sock, fd, 0, NULL, NULL, 1024);
				close(fd);
			}
		}

		break; //TODEL
	}
}


// TODO
// validate and parse pipelined request string
// example input:
// GET / HTTP/1.1\r\n
// Host: www.cs.ucsd.edu\r\n
// User-Agent: MyTester v1.0\r\n
// Cookie: 123\r\n
// GET /myimg.jpg HTTP/1.1\r\n
// Host: www.cs.ucsd.edu\r\n
// User-Agent: MyTester v1.0\r\n
// Cookie: 123\r\n
// My-header: mykey0123\r\n
// \r\n
std::vector<string> HttpdServer::parse_request(string req_str) {
	auto log = logger();
	log->info("parse request {}", req_str);
	std::vector<string> urls;
	urls.push_back(doc_root + "/index.html");
	// urls.push_back(doc_root + "/myhttpd/kitten.jpg");
	return urls;
}

// TODO
// check if there are any ".." in the url
bool HttpdServer::is_path_accessible(const string path) {
	auto log = logger();
	log->info("Path {} is accessible", path);
	return true;
}