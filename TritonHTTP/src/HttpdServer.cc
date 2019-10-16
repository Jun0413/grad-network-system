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


void HttpdServer::launch() {
	auto log = logger();

	log->info("Launching web server");
	log->info("Port: {}", port);
	log->info("doc_root: {}", doc_root);

	// Put code here that actually launches your webserver...

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

	while (1) {

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


void HttpdServer::handle_client(int clnt_sock) {
	auto log = logger();
	log->info("handle client socket: {}", clnt_sock);

	// where to put this param?
	const unsigned int MAX_RECV_BUF_SIZE = 4096;

	while (1) {
		std::string req_str;
		int bytes_recv = 0;
		// 1. get request string (close if timeout)
		do {
			std::vector<char> buffer(MAX_RECV_BUF_SIZE);
			bytes_recv = recv(clnt_sock, &buffer[0], buffer.size(), 0);
			// firstly check if it is timed out
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				log->info("receive time out");

				// TODO
				// close socket, send back a connection:close
				// and stop handling this client
				close(clnt_sock);

				return;
			}
			
			if (bytes_recv == -1) {
				log->error("receiv()");
				
				//TODO
				// close socket, (send error code?)
				// and stop handling this client
				close(clnt_sock);

				return;
			} else {
				req_str.append(buffer.cbegin(), buffer.cend());
			}
		} while (bytes_recv > 0);

		log->info("receive: {}", req_str);
		break;

		// 2. validate request string

		// 3. parse request string (possibly pipelined)

		// 4. locate files (mime types, security, ...)

		// 5. sendfile	
	}
}