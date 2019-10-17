#ifndef HTTPDSERVER_HPP
#define HTTPDSERVER_HPP

#include "inih/INIReader.h"
#include "logger.hpp"


#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <vector>


class HttpdServer {
public:
	HttpdServer(INIReader& t_config);

	void launch();

protected:
	INIReader& config;
	string port;
	string doc_root;

	void handle_client(int clnt_sock);

	void parse_request(const string& req_str, std::vector<string>& urls, std::vector<int>& codes);

	string convert_path(string path);
	std::vector<string> split(const string& str, const string& delim);
};

#endif // HTTPDSERVER_HPP
