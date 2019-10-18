#include <sysexits.h>


#include "logger.hpp"
#include "HttpdServer.hpp"


HttpdServer::HttpdServer(INIReader& t_config)
	: config(t_config) {
	auto log = logger();

	// load configs
	// TODO: wrap in a function?
	log->info("Loading configs...");
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

	std::string mmtype = config.Get("httpd", "mime_types", "");
	if (mmtype == "") {
		log->error("mime_types was not in the config file");
		exit(EX_CONFIG);
	}
	mime_types_file = mmtype;

	std::string servname = config.Get("httpd", "server_name", "");
	if (servname == "") {
		log->error("server_name was not in the config file");
		exit(EX_CONFIG);
	}
	server_name = servname;

	std::string s200msg = config.Get("httpd", "status_200_message", "");
	if (s200msg == "") {
		log->error("status_200_message was not in the config file");
		exit(EX_CONFIG);
	}
	status_200_message = s200msg;

	std::string s400msg = config.Get("httpd", "status_400_message", "");
	if (s400msg == "") {
		log->error("status_400_message was not in the config file");
		exit(EX_CONFIG);
	}
	status_400_message = s400msg;

	std::string s404msg = config.Get("httpd", "status_404_message", "");
	if (s404msg == "") {
		log->error("status_404_message was not in the config file");
		exit(EX_CONFIG);
	}
	status_404_message = s404msg;

	log->info("Server Name: {}", server_name);
	log->info("Mime Types File: {}", mime_types_file);
	log->info("Message of HTTP 200: {}", status_200_message);
	log->info("Message of HTTP 400: {}", status_400_message);
	log->info("Message of HTTP 404: {}", status_404_message);

	// construct mapping from .ext to mime_type
	load_mime_types();
}


// start TCP server and create a thread for each connection
void HttpdServer::launch() {
	auto log = logger();

	log->info("Launching web server");
	log->info("Port: {}", port);
	log->info("doc_root: {}", doc_root);
	
	// TODO: where to put these variables?
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

	// TODO: where to put this param?
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
					// TODO: send anything back?
					close(clnt_sock);
					return;
				}
				log->error("recv()");
				//TODO: send what kind of response?
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

		/////// a simple test case that works with client ///////////////
		// std::vector<int> status_codes;
		// status_codes.push_back(200);
		// std::vector<std::string> absolute_paths;
		// absolute_paths.push_back("<CHANGE_THIS_TO_ABSOLUTE_PATH_OF_A_FILE>");
		// send_response(clnt_sock, status_codes, absolute_paths);
		// continue;
		///////////////////////////////////////////////

		// 2. validate and parse request string (possibly pipelined)
		std::vector<string> urls;
		int statusCode = parse_request(req_str, urls);
		if(statusCode != 200){
			log->error("Invalid HTTP Request");
			// TODO
		}

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
// If valid request, return 200 and store urls into urls vertor
// Otherwise, return corresponding error code
// return -1 if "Connection" field is set to -1

// test cases
// std::vector<std::pair<string, int>> requests = {{"GET / HTTP/1.1\r\nHost: www.cs.ucsd.edu\r\nUser-Agent: MyTester v1.0\r\nCookie: 123\r\n\r\n", 200}, 
// 		{"GET /test1.jpg HTTP/1.1\r\nHost: www.cs.ucsd.edu\r\nUser-Agent: MyTester v1.0\r\nCookie: 123\r\n\r\nGET /test12.jpg HTTP/1.1\r\nHost: www.cs.ucsd.edu\r\nUser-Agent: MyTester v1.0\r\nCookie: 123\r\n\r\n", 200},
// 		{"GET /test2.jpg HTTP/1.1\r\nHost: www.cs.ucsd.edu\r\nUser-Agent: MyTester v1.0\r\nCookie: 123\r\n", 400},
// 		{"GET /test3.jpg HTTP/1.1\r\nHst: www.cs.ucsd.edu\r\nUser-Agent: MyTester v1.0\r\nCookie: 123\r\n\r\n", 400},
// 		{"GET /test4.jpg\r\nHost: www.cs.ucsd.edu\r\nUser-Agent: MyTester v1.0\r\nCookie: 123\r\n\r\n", 400}, 
// 		{"POST /test5.jpg HTTP/1.1\r\nHost: www.cs.ucsd.edu\r\nUser-Agent: MyTester v1.0\r\nCookie: 123\r\n\r\n", 400},
// 		{"GET /test6.jpg HTTP/1.1\r\nHost: www.cs.ucsd.edu\r\nUser-Agent: MyTester v1.0\r\nConnection: close\r\nCookie: 123\r\n\r\n", -1}, 
// 		{"GET /test7.jpg HTTP/1.0\r\nHost: www.cs.ucsd.edu\r\nUser-Agent: MyTester v1.0\r\nCookie: 123\r\n\r\n", 400}, 
// 		{"GET /test8.jpg HTTP/1.1\r\nHost: www.cs.ucsd.edu\r\nUser-Agent: MyTester: v1.0\r\nCookie: 123\r\n\r\n", 400},
// 		{"GET HTTP/1.0\r\nHost: www.cs.ucsd.edu\r\nUser-Agent: MyTester v1.0\r\nCookie: 123\r\n\r\n", 400}
// };
// for(const auto& p: requests){
// 	std::vector<string> urls;
// 	int statusCode = parse_request(p.first, urls);
// 	for(const auto& url : urls){
// 		log->info("Url: {}", url);
// 	}
// 	if(p.second != statusCode){
// 		log->error("Parse request failed, {}", p.first);
// 	}
// }

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
int HttpdServer::parse_request(const string& req_str, std::vector<string>& urls) {
	auto log = logger();
	log->info("Parse request: {}", req_str);
	std::vector<string> lines = split(req_str, "\r\n");
	if(lines.size() < 3 || lines[0].substr(0, 3) != "GET" || !lines.back().empty()){
		// HTTP request should have at least 3 lines, i.e. initial line, host field line and last empyt line
		log->error("Bad HTTP Request");
		return 400;
	}
	bool hasHostField = false, isNewRequest = true;
	for(int i = 0; i < (int)lines.size(); i++){
		const string& line = lines[i];
		log->info("Processing line: {}", line);
		if(line.empty()){
			// previous request ends
			if(!hasHostField){
				log->error("No Host field found in the HTTP request");
				return 400;
			}
			hasHostField = false;
			isNewRequest = true;
			continue;
		}
		if(isNewRequest){
			auto values = split(line, " ");
			if(values.size() != 3 || values[0] != "GET" || values[2] != "HTTP/1.1"){
				log->error("Bad request initial line: {}", line);
				return 400;
			}
			// should we check if url is valid here?
			urls.push_back(values[1]);
			isNewRequest = false;
		}else{
			auto values = split(line, ":\t");
			if(values.size() != 2){
				log->error("Bad request key-value pair: {}", line);
				for(const auto val : values){
					log->info("Values: {}", val);
				}
				return 400;
			}
			if(values[0] == "Host"){
				hasHostField = true;
			}else if(values[0] == "Connection"){
				if(values[1] == "close"){
					// close connection
					log->info("Close connection requested by client");
					// should we check this request has "Host" field here?
					return -1; // custom code
				}
			}
			// just ignore other request fields
			// RFC 2616 allows multiple header fields with same name under specific conditions, https://www.w3.org/Protocols/rfc2616/rfc2616-sec4.html#sec4.2
			// TritonHTTP specification: "You do not need to support duplicate keys in a single request. So, for example, requests will never have two Host headers"
		}
	}
	return 200;
}

// TODO
// check if 1. path exists 2. path is accessible
bool HttpdServer::is_path_accessible(const string path) {
	auto log = logger();
	log->info("Path {} is accessible", path);
	return true;
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

// negative status codes indicate close connection after processing
// absolute paths all must be valid
void HttpdServer::send_response(int clnt_sock, std::vector<int>& status_codes,
    std::vector<std::string>& absolute_paths) {
	
	auto log = logger();
	if (status_codes.size() != absolute_paths.size()) {
		log->error("send_response(): vector status_codes and absolute_paths must have same size");
		return;
	}

	bool should_close_connection = false;
	for (unsigned long i = 0; i < status_codes.size(); i++) {
		int status_code = status_codes[i];
		string absolute_path = absolute_paths[i];

		if (status_code < 0) {
			should_close_connection = true;
			status_code = -status_code;
		}
		
		// get file metadata for response fields
		struct stat f_attrb;
		stat(absolute_path.c_str(), &f_attrb);
		size_t f_size = f_attrb.st_size;
		char f_datetime[40] = {0};
        strftime(f_datetime, 40, "%a, %d %b %y %T %z", localtime(&f_attrb.st_mtime));

		auto last_dot_pos = absolute_path.find_last_of(".");
		if (last_dot_pos == string::npos) last_dot_pos = absolute_path.size();
		auto last_slash_pos = absolute_path.find_last_of("/"); // TODO: escape?
		string f_name = absolute_path.substr(last_slash_pos + 1, last_dot_pos);
		string f_ext = absolute_path.substr(last_dot_pos);
		
		log->info("File size: {}", f_size);
		log->info("File last modified date: {}", std::string(f_datetime));
		log->info("File name: {}", f_name);
		log->info("File extension: {}", f_ext);

		// construct the header
		std::string header = "HTTP/1.1";
		if (status_code == 200) {
			header += " " + std::to_string(status_code) + " " + status_200_message + "\r\n";
		} else if (status_code == 400) {
			header += " " + std::to_string(status_code) + " " + status_400_message + "\r\n";
		} else if (status_code == 404) {
			header += " " + std::to_string(status_code) + " " + status_404_message + "\r\n";
		}
		header += "Server: " + server_name + "\r\n";
		header += "Last-Modified: " + std::string(f_datetime) + "\r\n";
		header += "Content-Length: " + std::to_string(f_size) + "\r\n";
		header += "Content-Type: ";
		if (to_mime.find(f_ext) == to_mime.end()) header += "application/octet-stream\r\n\r\n";
		else header += to_mime[f_ext] + "\r\n\r\n";

		// TODO: reliability && limit of sendfile()?
		int fd = open(absolute_path.c_str(), O_RDONLY);
		if (fd == -1) { // should not happen
			log->error("open() file {}", absolute_path);
			continue;
		}
		// first, send header all at one time
		const char* header_buffer = header.c_str();
		send(clnt_sock, header_buffer, strlen(header_buffer), 0);
		// then, send the file
		off_t sf_len = 0;
		// TODO: sendfile() below is platform dependent
		if (sendfile(fd, clnt_sock, 0, &sf_len, nullptr, 0) == -1) {
			log->error("sendfile(): unsuccessful");
			close(fd);
			continue;
		}
		close(fd);

		if (should_close_connection) {
			close(clnt_sock);
			return;
		}
	}
}


// load mime type file into a map, config exit if error
// e.g. ".jpg" -> "image/jpeg"
void HttpdServer::load_mime_types() {
	auto log = logger();
	std::string line;
	std::ifstream in_file(mime_types_file);
	if (!in_file.good()) {
		log->error("Mime type file {} not found", mime_types_file);
		exit(EX_CONFIG);
	}
	while (getline(in_file, line)) {
		std::vector<string> pair_ = split(line, " ");
		to_mime[pair_[0]] = pair_[1];
	}
}