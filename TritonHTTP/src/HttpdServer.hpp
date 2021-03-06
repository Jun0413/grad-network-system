#ifndef HTTPDSERVER_HPP
#define HTTPDSERVER_HPP

#include "inih/INIReader.h"
#include "logger.hpp"


#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <fstream>
#include <map>
#include <netdb.h>
#include <string>
#include <sysexits.h>
#include <sys/sendfile.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <time.h>
#include <unistd.h>
#include <vector>
#include <wordexp.h>


class HttpdServer {
public:
	HttpdServer(INIReader& t_config);

	void launch();

protected:
	INIReader& config;
	string port;
	string doc_root;
	string mime_types_file;

	std::map<string, string> to_mime;

	void handle_client(int clnt_sock);

	void parse_request(const string& req_str, std::vector<string>& urls, std::vector<int>& codes);

	string convert_path(string path);
	std::vector<string> split(const string& str, const string& delim);

	bool send_response(int clnt_sock, std::vector<int>& status_codes, std::vector<std::string>& absolute_paths);

	void load_mime_types();
};

#endif // HTTPDSERVER_HPP
