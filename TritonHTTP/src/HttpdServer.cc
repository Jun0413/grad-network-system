#include <sysexits.h>
#include <sys/stat.h>
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
	char* realdr = realpath(dr.c_str(), NULL);
	if(realdr == NULL){
		log->error("Invalid doc_root: {}", dr);
		exit(EX_CONFIG);
	}
	doc_root = string(realdr);
	free(realdr);
}


// start TCP server and create a thread for each connection
void HttpdServer::launch() {
	auto log = logger();

	log->info("Launching web server");
	log->info("Port: {}", port);
	log->info("doc_root: {}", doc_root);
	
	std::vector<std::pair<string, std::vector<int>>> requests = {
        {"GET / HTTP/1.1\r\nHost: www.cs.ucsd.edu\r\nUser-Agent: MyTester v1.0\r\nCookie: 123\r\n\r\n", {200}}, 
		{"GET /index.html HTTP/1.1\r\nHost: www.cs.ucsd.edu\r\nUser-Agent: MyTester v1.0\r\nCookie: 123\r\n\r\nGET /kitten.jpg HTTP/1.1\r\nHost: www.cs.ucsd.edu\r\nUser-Agent: MyTester v1.0\r\nCookie: 123\r\n\r\n", {200, 200}},
		{"GET /index.html HTP/1.1\r\nHost: www.cs.ucsd.edu\r\nUser-Agent: MyTester v1.0\r\nCookie: 123\r\n\r\nGET /kitten.jpg HTTP/1.1\r\nHost: www.cs.ucsd.edu\r\nUser-Agent: MyTester v1.0\r\nCookie: 123\r\n\r\nGET /kitten0.jpg HTTP/1.1\r\nHost: www.cs.ucsd.edu\r\nUser-Agent: MyTester v1.0\r\nCookie: 123\r\n\r\n", {400, 200, 404}},
        {"GET /subdir1/index.html HTTP/1.1\r\nHost: www.cs.ucsd.edu\r\nUser-Agent: MyTester v1.0\r\nCookie: 123\r\n\r\n", {200}}, 
        {"GET /subdir1/../index.html HTTP/1.1\r\nHost: www.cs.ucsd.edu\r\nUser-Agent: MyTester v1.0\r\nCookie: 123\r\n\r\n", {200}}, 
        {"GET /subdir2/../index.html HTTP/1.1\r\nHost: www.cs.ucsd.edu\r\nUser-Agent: MyTester v1.0\r\nCookie: 123\r\n\r\n", {404}}, 
        {"GET /../kitten.jpg HTTP/1.1\r\nHost: www.cs.ucsd.edu\r\n\r\n", {404}},
        {"GET /kitten.jpg HTTP/1.1\r\nHost: www.cs.ucsd.edu\r\n\r\nGET /kitten2.jpg HTTP/1.1\r\nHost: www.cs.ucsd.edu\r\n\r\nGET /UCSD.jpg HTTP/1.1\r\nHost: www.cs.ucsd.edu\r\n\r\nGET /UCSD_Seal.png HTTP/1.1\r\nHost: www.cs.ucsd.edu\r\n\r\nGET /../kitten.jpg HTTP/1.1\r\nHost: www.cs.ucsd.edu\r\nConnection: close\r\n\r\n", {200, 404, 404, 200, -404}},      
        {"GET /kitten0.jpg HTTP/1.1\r\nHost: www.cs.ucsd.edu\r\nUser-Agent: MyTester v1.0\r\nCookie: 123\r\n", {404}},
        {"GET /test3.jpg HTTP/1.1\r\nHst: www.cs.ucsd.edu\r\nUser-Agent: MyTester v1.0\r\nCookie: 123\r\n\r\n", {400}},
        {"GET /test4.jpg\r\nHost: www.cs.ucsd.edu\nUser-Agent: MyTester v1.0\r\nCookie: 123\r\n\r\n", {400}}, 
        {"POST /test5.jpg HTTP/1.1\r\nHost: www.cs.ucsd.edu\r\nUser-Agent: MyTester v1.0\r\nCookie: 123\r\n\r\n", {400}}
	};

	for(const auto& p: requests){
		std::vector<string> urls;
		std::vector<int> codes;
		parse_request(p.first, urls, codes);
		for(int i = 0; i < (int)codes.size(); i++){
			if(codes[i] != p.second[i]){
				log->error("Wrong status code: {}, expected: {}", codes[i], p.second[i]);
			}
		}
		for(const auto& url : urls){
			log->info("Url: {}", url);
		}
	}
	/*
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
	*/
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
		// std::vector<string> urls;
		// int statusCode = parse_request(req_str, urls);
		// if(statusCode != 200){
		// 	log->error("Invalid HTTP Request");
		// 	// TODO
		// }

		// 3. locate files (mime types, security, ...)
		// and send files
		// for (string url : urls) {
		// 	log->info("retrieving file {}", url);
		// 	if (is_path_accessible(url)) {
		// 		char* abs_path = realpath(url.c_str(), NULL);
		// 		log->info("absolute path: {}", abs_path);
		// 		int fd = open(abs_path, O_RDONLY);
		// 		if (fd == -1) {
		// 			log->error("open() file");
		// 			continue;
		// 		}
		// 		// sendfile(clnt_sock, fd, 0, NULL, NULL, 1024);
		// 		close(fd);
		// 	}
		// }

		break; //TODEL
	}
}



// TODO
// validate and parse pipelined request string
// If valid requests, codes set to 200 and store urls into urls vertor
// If request's connection field set to close, codes set to negative normal status code, store urls into urls vector
// Otherwise, codes set to corresponding error code (400, 404), the priority is 400 > 404 > 200
// Assume that requests have correct structure, i.e. every request ended with an empty line (\r\n)

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
void HttpdServer::parse_request(const string& req_str, std::vector<string>& urls, std::vector<int>& codes) {
	auto log = logger();
	log->info("Parse request: {}", req_str);
	std::vector<string> lines = split(req_str, "\r\n");
	bool hasHostField = false, isNewRequest = true, closeConnection = false;
	for(int i = 0; i < (int)lines.size(); i++){
		const string& line = lines[i];
		log->info("Processing line: {}", line);
		if(line.empty()){
			// previous request ends
			if(!hasHostField){
				log->error("No Host field found in the HTTP request");
				codes.back() = 400;
			}
			if(closeConnection){
				codes.back() = -codes.back();
			}
			if(urls.size() < codes.size()){
				urls.push_back("");
			}
			hasHostField = false;
			isNewRequest = true;
			closeConnection = false;
			continue;
		}
		if(isNewRequest){
			auto values = split(line, " ");
			if(values.size() != 3 || values[0] != "GET" || values[1].empty() || values[1][0] != '/' || values[2] != "HTTP/1.1"){
				log->error("Bad request initial line: {}", line);
				urls.push_back("");
				codes.push_back(400);
			}else{
				string path = convert_path(values[1]);
				if(path.empty()){
					codes.push_back(404);
					urls.push_back("");
				}else{
					codes.push_back(200);
					urls.push_back(path);
				}
			}
			isNewRequest = false;
		}else{
			if(codes.back() == 400){
				// no need to continue processing this request if status code is already 400
				continue;
			}
			auto values = split(line, ":\t");
			if(values.size() != 2){
				log->error("Bad request key-value pair: {}", line);
				for(const auto val : values){
					log->info("Values: {}", val);
				}
				codes.back() = 400;
				continue;
			}
			if(values[0] == "Host"){
				hasHostField = true;
			}else if(values[0] == "Connection"){
				if(values[1] == "close"){
					// close connection
					log->info("Close connection requested by client");
					closeConnection = true;
				}
			}
			// just ignore other request fields
			// RFC 2616 allows multiple header fields with same name under specific conditions, https://www.w3.org/Protocols/rfc2616/rfc2616-sec4.html#sec4.2
			// TritonHTTP specification: "You do not need to support duplicate keys in a single request. So, for example, requests will never have two Host headers"
		}
	}
}

// convert relative into absolute path, and check if the path is valid
// return absolute path if valid, otherwise return empty string
string HttpdServer::convert_path(string path) {
	auto log = logger();
	log->info("Convert path: {}, len = {}", path, path.size());
	if(path.empty()) return "";
	if(path == "/"){
		log->info("Convert path: default path detected");
		path = "/index.html";
	}
	path = doc_root + path;
	char* abs_path = realpath(path.c_str(), NULL);
	if(abs_path != NULL){
		log->info("Converted: {}", abs_path);
		string ans(abs_path);
		free(abs_path);
		if(ans.substr(0, doc_root.size()) != doc_root){
			// || access(ans.c_str(), F_OK) == -1
			// file must exist, otherwise realpath() will return NULL
			// escape root directory
			return "";
		}
		return ans;
	}else{
		// realpath() will return NULL if convert failed
		free(abs_path);
		return "";
	}
	return "";
}

std::vector<string> HttpdServer::split(const string& str, const string& delim){
	// auto log = logger();
	// log->info("Split: {}, delim: {}", str, delim);
	// Split str by delim
	// Reference: https://stackoverflow.com/a/7408245
	std::vector<string> res;
	std::size_t start = 0, end = 0, searchStart = 0;
	while((end = str.find_first_of(delim, searchStart)) != string::npos){
		// log->info("Start: {}, end: {}, str[end]:{}", start, end, (int)str[end]);
		if(str[end] != delim[0]){
			// only useful when delim = "\r\n". In Unix system, it will match a single '\n'
			searchStart = end + 1;
			continue;
		}
		res.push_back(str.substr(start, end - start));
		searchStart = start = end + delim.size();
	}
	if(start < str.size()){
		res.push_back(str.substr(start));
	}
	return res;
}