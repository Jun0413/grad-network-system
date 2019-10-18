#include "logger.hpp"
#include "HttpdServer.hpp"


HttpdServer::HttpdServer(INIReader& t_config)
	: config(t_config) {
	auto log = logger();

	// load configs
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
    
	char* realdr = realpath(dr.c_str(), NULL);
	if(realdr == NULL){
		log->error("Invalid doc_root: {}", dr);
		exit(EX_CONFIG);
	}
	doc_root = string(realdr);
	free(realdr);

	std::string mmtype = config.Get("httpd", "mime_types", "");
	if (mmtype == "") {
		log->error("mime_types was not in the config file");
		exit(EX_CONFIG);
	}
	mime_types_file = mmtype;

	log->info("Mime Types File: {}", mime_types_file);

	// construct mapping from .ext to mime_type
	load_mime_types();
}


// start TCP server and create a thread for each connection
void HttpdServer::launch() {
	auto log = logger();

	log->info("Launching web server");
	log->info("Port: {}", port);
	log->info("doc_root: {}", doc_root);

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

	const int MAX_RECV_BUF_SIZE = 4096;

	while (true) {
		std::string req_str;
		int bytes_recv = 0;
		// 1. get request string (close if timeout)
		while (true) {
			char buffer[MAX_RECV_BUF_SIZE] = {0};
			bytes_recv = recv(clnt_sock, buffer, MAX_RECV_BUF_SIZE, 0);

			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				log->info("recv() timed out");
				if (req_str.size() > 0) break;
				close(clnt_sock);
				return;
			}

			if (bytes_recv <= 0) {
				close(clnt_sock);
				return;
			}

			if (bytes_recv > 0) {
				req_str.append(buffer, bytes_recv);
				if (req_str.size() >= 4
					&& req_str[req_str.size() - 1] == '\n'
					&& req_str[req_str.size() - 2] == '\r'
					&& req_str[req_str.size() - 3] == '\n'
					&& req_str[req_str.size() - 4] == '\r'
				) break;
			}
		}

		log->info("receive: {}", req_str);

		// 2. validate and parse request string (possibly pipelined)
		std::vector<string> urls;
		std::vector<int> status_codes;
		parse_request(req_str, urls, status_codes);

		// 3. retrieve files and send files
		if(send_response(clnt_sock, status_codes, urls)) {
			close(clnt_sock);
			return;
		}
	}
}


// validate and parse pipelined request string
// If valid requests, codes set to 200 and store urls into urls vertor
// If request's connection field set to close, codes set to negative normal status code, store urls into urls vector
// Otherwise, codes set to corresponding error code (400, 404), the priority is 400 > 404 > 200
// Assume that requests have correct structure, i.e. every request ended with an empty line (\r\n)
void HttpdServer::parse_request(const string& req_str, std::vector<string>& urls, std::vector<int>& codes) {
	auto log = logger();
	log->info("Parse request: {}", req_str);
	string error400page = doc_root + "\\error400.html", error404page = doc_root + "\\error404.html";
	std::vector<string> lines = split(req_str, "\r\n");
	bool hasHostField = false, isNewRequest = true, closeConnection = false;
	for(int i = 0; i < (int)lines.size(); i++){
		const string& line = lines[i];
		log->info("Processing line: {}, len = {}", line, line.size());
		if(line.empty()){
			// previous request ends
			if(!hasHostField){
				log->error("No Host field found in the HTTP request");
				codes.back() = 400;
				urls.back() = error400page;
			}
			if(closeConnection){
				codes.back() = -codes.back();
			}
			if(urls.size() < codes.size()){
				// should not enter here
				log->info("End of request, should not occur");
				urls.push_back(error400page);
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
				urls.push_back(error400page);
				codes.push_back(400);
			}else{
				string path = convert_path(values[1]);
				if(path.empty()){
					codes.push_back(404);
					urls.push_back(error404page);
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
			auto values = split(line, ": ");
			if(values.size() != 2){
				log->error("Bad request key-value pair: {}", line);
				for(const auto val : values){
					log->info("Values: {}", val);
				}
				codes.back() = 400;
				urls.back() = error400page;
				continue;
			}
			if(values[0] == "Host"){
				log->info("Find Host field: {}", values[1]);
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
	if(!isNewRequest){
		// last requestv is not full
		if(urls.size() < codes.size()){
			urls.push_back("");
		}
		if(!codes.empty()){
			urls.back() = error400page;
			codes.back() = 400;
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
	// Split str by delim
	// Reference: https://stackoverflow.com/a/7408245
	std::vector<string> res;
	std::size_t start = 0, end = 0, searchStart = 0;
	while((end = str.find_first_of(delim, searchStart)) != string::npos){
		bool matched = true;
		int i = end;
		for(; i < (int)str.size() && i < (int)(end + delim.size()); i++){
			if(str[i] != delim[i-end]){
				matched = false;
				break;
			}
		}
		if(!matched && i != (int)(end + delim.size())){
			// portability problem under different platforms, e.g. "\r\n"
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
// return true if the connection should be closed
bool HttpdServer::send_response(int clnt_sock, std::vector<int>& status_codes,
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
		auto last_slash_pos = absolute_path.find_last_of("/");
		string f_name = absolute_path.substr(last_slash_pos + 1, last_dot_pos);
		string f_ext = absolute_path.substr(last_dot_pos);
		
		log->info("File size: {}", f_size);
		log->info("File last modified date: {}", std::string(f_datetime));
		log->info("File name: {}", f_name);
		log->info("File extension: {}", f_ext);

		// construct the header
		std::string header = "HTTP/1.1";
		if (status_code == 200) {
			header += " " + std::to_string(status_code) + " OK\r\n";
		} else if (status_code == 400) {
			header += " " + std::to_string(status_code) + " Bad Request\r\n";
		} else if (status_code == 404) {
			header += " " + std::to_string(status_code) + " Not Found\r\n";
		}
		header += "Server: TritonServer 1.0\r\n";
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
		// TODO: sendfile() below is platform dependent
		if (sendfile(clnt_sock, fd, NULL, f_size) == -1) {
			log->error("sendfile(): unsuccessful");
			close(fd);
			continue;
		}
		close(fd);

		if (should_close_connection) return true;
	}

	return false;
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