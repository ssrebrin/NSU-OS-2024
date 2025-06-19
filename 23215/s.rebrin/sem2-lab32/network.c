#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netdb.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <fcntl.h>
#include "network.h"

void error(const char* msg);

int con_to_host(int* sockfd, const char* full_host) {
    errno = 0;

    char host[256];
    char port_str[6] = "80";  // Default port: 80

  
    const char* colon = strchr(full_host, ':');
    if (colon) {
        size_t host_len = colon - full_host;
        if (host_len >= sizeof(host)) {
            fprintf(stderr, "Hostname too long\n");
            return 1;
        }
        strncpy(host, full_host, host_len);
        host[host_len] = '\0';

        int port = atoi(colon + 1);
        if (port <= 0 || port > 65535) {
            fprintf(stderr, "Invalid port number\n");
            return 1;
        }
        snprintf(port_str, sizeof(port_str), "%d", port);
    }
    else {
        strncpy(host, full_host, sizeof(host) - 1);
        host[sizeof(host) - 1] = '\0';
    }

    struct addrinfo hints = { 0 }, * res = NULL, * p;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    int err = getaddrinfo(host, port_str, &hints, &res);
    if (err != 0) {
        fprintf(stderr, "DNS resolution failed for %s: %s\n", host, gai_strerror(err));
        return 1;
    }

    for (p = res; p != NULL; p = p->ai_next) {
        *sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (*sockfd < 0) continue;

        if (connect(*sockfd, p->ai_addr, p->ai_addrlen) == 0) break; // Успех

        close(*sockfd);
        *sockfd = -1;
    }

    freeaddrinfo(res);

    if (*sockfd < 0) {
        perror("Unable to connect");
        return 1;
    }

    return 0;
}

void con_to_cli(int* sockfd) {
    struct sockaddr_in serv_addr;

    *sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (*sockfd < 0)
        error("Opening socket error");

    int opt = 1;
    setsockopt(*sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    //int rcvbuf = 16; // 256 байт
    //setsockopt(*sockfd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));

    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    serv_addr.sin_port = htons(9002);

    if (bind(*sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0)
        error("Binding error");
}

void fix_request_line(char* buf) {
    char* line_end = strstr(buf, "\r\n");
    if (!line_end) return;

    char* http_pos = strstr(buf, "http://");
    if (!http_pos || http_pos > line_end) return;

    char* path_start = strchr(http_pos + strlen("http://"), '/');
    if (!path_start || path_start > line_end) return;

    char* version_start = strchr(path_start, ' ');
    if (!version_start || version_start > line_end) return;

    size_t method_len = 0;
    if (strncmp(buf, "GET ", 4) == 0) method_len = 4;
    else return;

    size_t path_len = version_start - path_start;
    size_t version_len = line_end - version_start;

    char new_line[1024];
    int written = snprintf(new_line, sizeof(new_line), "%.*s %.*s%.*s\r\n",
        (int)method_len, buf,
        (int)path_len, path_start,
        (int)version_len, version_start);

    if (written <= 0 || (size_t)written >= sizeof(new_line)) return;

    size_t tail_len = strlen(line_end + 2);
    memmove(buf + written, line_end + 2, tail_len + 1);
    memcpy(buf, new_line, written);
}


void set_nonblocking(int cl_fd) {
    int flags = fcntl(cl_fd, F_GETFL, 0);
    if (flags == -1) {
        perror("fcntl(F_GETFL) failed");
        return;
    }

    if (fcntl(cl_fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        perror("fcntl(F_SETFL) failed");
    }
}

void parse_http_request(const char* request, char* host, int* con) {
    char method[16], path[256], version[16];

    sscanf(request, "%15s %255s %15s", method, path, version);

    const char* host_header = strstr(request, "Host:");
    if (host_header) {
        sscanf(host_header, "Host: %255s", host);
        char* end = host + strlen(host) - 1;
        while (end >= host && (*end == '\r' || *end == '\n')) *end-- = '\0';
    }

    *con = 0;
    const char* connection_header = strstr(request, "\r\nConnection:");
    if (connection_header) {
        connection_header += strlen("\r\nConnection:");
        while (*connection_header && isspace((unsigned char)*connection_header)) connection_header++;
        if (strncasecmp(connection_header, "close", 5) == 0) {
            *con = 1;
        }
    }
}

int get_content_length_from_headers(const char* headers) {
    const char* content_length_header = "Content-Length:";
    const char* ptr = strstr(headers, content_length_header);

    if (ptr == NULL) {
        return -1;
    }

    ptr += strlen(content_length_header);
    while (*ptr && isspace(*ptr)) {
        ptr++;
    }

    int length = 0;
    while (*ptr && isdigit(*ptr)) {
        length = length * 10 + (*ptr - '0');
        ptr++;
    }

    return length;
}

void parse_headers(const char* headers, int* content_length, int* cache_live, int* status) {
    *content_length = -1;
    *cache_live = -1;
    *status = -1;

    const char* content_length_ptr = strstr(headers, "Content-Length:");
    if (content_length_ptr != NULL) {
        content_length_ptr += strlen("Content-Length:");
        while (*content_length_ptr && isspace(*content_length_ptr)) {
            content_length_ptr++;
        }

        *content_length = 0;
        while (*content_length_ptr && isdigit(*content_length_ptr)) {
            *content_length = *content_length * 10 + (*content_length_ptr - '0');
            content_length_ptr++;
        }
    }

    const char* cache_control_ptr = strstr(headers, "Cache-Control:");
    if (cache_control_ptr != NULL) {
        const char* max_age_ptr = strstr(cache_control_ptr, "max-age=");
        if (max_age_ptr != NULL) {
            max_age_ptr += strlen("max-age=");

            *cache_live = 0;
            while (*max_age_ptr && isdigit(*max_age_ptr)) {
                *cache_live = *cache_live * 10 + (*max_age_ptr - '0');
                max_age_ptr++;
            }
        }
    }

    const char* http_version_ptr = strstr(headers, "HTTP/");
    if (http_version_ptr != NULL) {
        while (*http_version_ptr && !isspace(*http_version_ptr)) {
            http_version_ptr++;
        }
        while (*http_version_ptr && isspace(*http_version_ptr)) {
            http_version_ptr++;
        }

        *status = 0;
        while (*http_version_ptr && isdigit(*http_version_ptr)) {
            *status = *status * 10 + (*http_version_ptr - '0');
            http_version_ptr++;
        }
    }
}