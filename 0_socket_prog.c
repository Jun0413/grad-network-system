#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/errno.h>

// 1
/**
 * int getaddrinfo(
 *     const char *node,               // ip or "www.abc.com"
 *     const char *service,            // "http" or port number
 *     const struct addrinfo *hints,
 *     struct addrinfo **res
 * )
 */


// 2
// show IP addresses for a host given on the command line
int show_ip(int argc, char *argv[]) {
    struct addrinfo hints, *res, *p;
    int status;
    char ipstr[INET6_ADDRSTRLEN];

    if (argc != 2) {
        fprintf(stderr, "usage: 0_socket_prog <hostname>\n");
        return 1;
    }

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    if ((status = getaddrinfo(argv[1], NULL, &hints, &res)) != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(status));
        return 2;
    }

    printf("ip addresses for %s:\n\n", argv[1]);

    for (p = res; p != NULL; p = p->ai_next) {

        void *addr;
        char *ipver;

        if (p->ai_family == AF_INET) {
            struct sockaddr_in *ipv4 = (struct sockaddr_in *) p->ai_addr;
            addr = &(ipv4->sin_addr);
            ipver = "IPv4";
        } else if (p->ai_family == AF_INET6) {
            struct sockaddr_in *ipv6 = (struct sockaddr_in *) p->ai_addr;
            addr = &(ipv6->sin_addr);
            ipver = "IPv6";
        }

        inet_ntop(p->ai_family, addr, ipstr, sizeof ipstr);
        printf("    %s: %s\n\n", ipver, ipstr);
    }

    freeaddrinfo(res);

    return 0;
}


// 3
void bind_on_port(const char *port) {
    struct addrinfo hints, *res;
    
    memset(&hints, 0, sizeof hints); // clear hints
    hints.ai_family = AF_UNSPEC; // dont care v4 or v6
    hints.ai_socktype = SOCK_STREAM; // TCP
    hints.ai_flags = AI_PASSIVE; // fill in localhost IP

    getaddrinfo(NULL, port, &hints, &res);

    int sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    bind(sockfd, res->ai_addr, res->ai_addrlen);
}

// IPv4 specific and does not use getaddrinfo()
// which is what I did for ftp implementation
void deprec_bind_on_port(int port) {
    int sockfd;
    struct sockaddr_in addr;

    sockfd = socket(PF_INET, SOCK_STREAM, 0);

    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = inet_addr("127.0.0.1"); // alternative: INADDR_ANY
    memset(addr.sin_zero, '\0', sizeof addr.sin_zero);

    bind(sockfd, (struct sockaddr *) &addr, sizeof addr);
}


// 4
void connect_example() {
    struct addrinfo hints, *res;

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    
    getaddrinfo("www.google.com", "80", &hints, &res);

    int sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);

    if (connect(sockfd, res->ai_addr, res->ai_addrlen) == -1) {
        printf("failed to connect, errno: %d\n", errno);
    }
}

// 5
// order of listening
// getaddrinfo() -> socket() -> bind() -> listen() -> accept()
// note that accept() is about accepting a new sockfd
// as such, the old one is still listening on the port
// while the new fd is used to send and receive data
// Sample code below:
void accept_example() {
    struct sockaddr_storage their_addr;
    socklen_t addr_size = sizeof their_addr;
    struct addrinfo hints, *res;
    int old_sockfd, new_sockfd;

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    getaddrinfo(NULL, "6324", &hints, &res);

    old_sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);

    bind(old_sockfd, res->ai_addr, res->ai_addrlen);
    
    listen(old_sockfd, 5);

    new_sockfd = accept(sockfd, (struct sockaddr *) &their_addr, addr_size);

    // ...ready to communicate on the new socket file descriptor
    // ...
    // ...
}


// 6
// int send(int sockfd, const void *msg, int len, int flags)
// int recv(int sockfd, void *buf, int len, int flags)
//
// int sendto(int sockfd, const void *msg, int len, unsigned int flags,
//            const struct sockaddr *to, socklen_t tolen)
// int recvfrom(int sockfd, void *buf, int len, unsigned int flags,
//            struct sockaddr *from, socklen_t fromlen)
//
// Note that if connect() a SOCK_DGRAM, send() and recv() can still
// be used for all transactions, the packets still use UDP,
// but the socket interface will automatically add src && dest information


// 7
// close(sockfd): to prevent further RW to the socket
// shutdown(sockfd, how) <-- just more controls on HOW to close the fd
//                  how    meaning
//                   0     further recv NOT allowed
//                   1     further send NOT allowed
//                   2     ~close()
//
//
// lastly,
// getpeername(int sockfd, struct sockaddr *addr, int *addrlen)
// gethostname(char *hostname, size_t size)

int main(int argc, char *argv[]) {
    // return show_ip(argc, argv);

    // bind_on_port("3480");
    // deprec_bind_on_port(3481);

    connect_example();
}