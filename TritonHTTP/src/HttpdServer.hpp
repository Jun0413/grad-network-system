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

	std::vector<string> parse_request(string req_str);

	bool is_path_accessible(const string path);
};

#endif // HTTPDSERVER_HPP
