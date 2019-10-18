#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>

#define AMSG "a message"
#define PORT "8080"
#define FILE_NAME "response"

#define CHUNKBYTES 512

void * get_inaddr(struct sockaddr *sa);

int main(void) {

    int sockfd;
    int rv, ret;
    struct addrinfo hints, *servinfo, *p;
    ssize_t bytes_read;
    char ipstr[INET6_ADDRSTRLEN];
    const char *amsg = AMSG;
    FILE *fp;

    // 1. getaddrinfo of server
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    
    if ((rv = getaddrinfo(NULL, PORT, &hints, &servinfo)) != 0) {
        fprintf(stderr, "getaddrinfo(): %s\n", gai_strerror(rv));
        return 1;
    }

    // 2. create socket and connect to the first server addr
    for (p = servinfo; p != NULL; p = p->ai_next) {
        if ((sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
            perror("socket creation");
            continue;
        }

        if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
            close(sockfd);
            perror("connection");
            continue;
        }

        break;
    }

    if (p == NULL) { // none of servaddr worked
        fprintf(stderr, "failed to connect.\n");
        return 2;
    }

    // 3. display connected server ip
    inet_ntop(p->ai_family, get_inaddr((struct sockaddr *) p->ai_addr), ipstr, sizeof ipstr);
    printf("Connected to %s\n", ipstr);

    freeaddrinfo(servinfo);

    // 4. send a message
    ret = send(sockfd, amsg, strlen(amsg), 0);
    if (ret != strlen(amsg)) {
        fprintf(stderr, "failed to send PID.\n");
        return 3;
    }

    // 5. receive file from server chunk by chunk
    printf("Receiving file... \n");
    fp = fopen(FILE_NAME, "w");
    do {
        char buf[CHUNKBYTES];
        bytes_read = recv(sockfd, buf, CHUNKBYTES - 1, 0);
        if (bytes_read < 0) {
            close(sockfd);
            fprintf(stderr, "failed to receive.\n");
            return 3;
        }
        if (bytes_read > 0) {
            buf[bytes_read] = '\0';
            fputs(buf, stdout);
            fputs(buf, fp);
        }
    } while (bytes_read > 0);
    fclose(fp);

    close(sockfd);

    return 0;
}

// return the sin_addr (network address)
void * get_inaddr(struct sockaddr *sa) {
    if (sa->sa_family == AF_INET) {
        return &(((struct sockaddr_in *) sa)->sin_addr);
    }
    return &(((struct sockaddr_in6 *) sa)->sin6_addr);
}
