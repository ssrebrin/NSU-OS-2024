#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <time.h>
#include <fcntl.h>
#include <signal.h>
#include <errno.h>
#include <pthread.h>
#include "network.h"
#include "cache.h"
#include "thread.h"

#define BUFFER_SIZE 4096
#define HOST "127.0.0.1:6000"
#define LOG 1

int sockfd;
int server_socket;
client* client_head = NULL;
time_t last_log;
int lg = 1;
pthread_mutex_t mut;
pthread_mutexattr_t attr;
pthread_mutex_t mut_cache;
pthread_mutexattr_t attr_cache;

void logs() {
    if (LOG) {
        pthread_mutex_lock(&mut);
        time_t lst = time(NULL);
        last_log = lst;
        client* cur = client_head;
        printf("\nClients log:\n");
        if (!cur) {
            printf(">No clients\n");
            lg = 0;
        }
        else {
            while (cur) {
                printf(">%s\n", cur->host ? cur->host : "(no host)");
                cur = cur->next;
                lg = 1;
            }
        }
        printf("\nCache log:\n");
        cache* cc = cache_head;
        if (!cc) {
            printf("==============================================\nNo cache\n==============================================\n");
        }
        else {
            while (cc) {
                if (cc->working)
                printf("====================Caching===================\n%s==============================================\n", cc->request);
                else
                printf("====================Caching===================\n%s==============================================\n", cc->request);
                cc = cc->next;
            }
        }
        printf("\n");
        pthread_mutex_unlock(&mut);
    }
}

void signal_log(int sig) {
    logs();
    signal(SIGQUIT, signal_log);
}

void signal_handler(int sig) {
    close(server_socket);
    client* cur = client_head;
    while (cur) {
        client* next = cur->next;
        close(cur->cli_fd);

        if (cur->inet_fd > 0) close(cur->inet_fd);
        if (cur->host) free(cur->host);
        if (cur->headers_collectors) free(cur->headers_collectors);
        free(cur);
        cur = next;
    }
    cache* cache_cur = cache_head;
    while (cache_cur) {
        cache* next = cache_cur->next;
        remove_from_cache(cache_cur);
        cache_cur = next;
    }
    exit(0);
}

void error(const char* msg) {
    perror(msg);
    signal_handler(SIGINT); 
}

client* add(int cl_fd) {
    client* a = malloc(sizeof(client));
    if (!a) return a;

    pthread_mutex_lock(&mut);
    a->cli_fd = cl_fd;
    set_nonblocking(cl_fd);
    a->next = NULL;
    a->tot = 0;
    a->len = 0;
    a->writing = 0;
    a->header_len = 0;
    a->headers_len = 0;
    a->writing_to_client = 0;
    a->writing_to_client_total = 0;
    a->using_cache = 0;
    a->caching = 0;
    a->collect_headers = 0;
    a->tunneling = 0; 
    a->headers_collectors = malloc(4096);
    a->host = malloc(BUFFER_SIZE);

    if (!a->host || !a->headers_collectors) {
        free(a->host);
        free(a->headers_collectors);
        free(a);
        return NULL;
    }
    a->host[0] = '\0';
    a->inet_fd = 0;
    a->cur_cache = NULL;
    a->cur_data = NULL;
    a->last_activity = time(NULL);

    if (!client_head) {
        client_head = a;
    }
    else {
    printf("%s\n", client_head->host);
        client* t = client_head;
        while (t->next) t = t->next;
        t->next = a;
    }
    pthread_mutex_unlock(&mut);
    return a;
}


int main() {
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK);
    pthread_mutex_init(&mut, &attr);
    pthread_mutexattr_init(&attr_cache);
    pthread_mutexattr_settype(&attr_cache, PTHREAD_MUTEX_ERRORCHECK);
    pthread_mutex_init(&mut_cache, &attr_cache);
    last_log = time(NULL);
    signal(SIGQUIT, signal_log);

    con_to_cli(&server_socket);
    if (listen(server_socket, 5) < 0) {
        error("listen error");
    }

    signal(SIGINT, signal_handler);

    printf("\nServer starting\n");

    while (1) {

        int client_socket = accept(server_socket, NULL, NULL);
        if (client_socket < 0) {
            perror("accept");
            continue;
        }
        printf(">New client %d\n", client_socket);
        

        thread_data* dat = (thread_data*)malloc(sizeof(thread_data));
        dat->cl = add(client_socket);
        dat->cl_h = &client_head;
        dat->mut = &mut;
        dat->mut_cac = &mut_cache;

        int code = pthread_create(&dat->cl->thr, NULL, cli_thread, dat);
        if (code != 0) {
            char buf[256];
            strerror_r(code, buf, sizeof buf);
            fprintf(stderr, "Creating thread: %s\n", buf);
            exit(1);
        }

    }
        close(server_socket);
        return 0;
}