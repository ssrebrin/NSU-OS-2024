#ifndef THREAD_H
#define THREAD_H

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
#include <stdatomic.h>
#include "network.h"
#include "cache.h"

extern atomic_int inter;
extern pthread_cond_t cond_var;

typedef struct thread_data {
    client* cl;
    client** cl_h;
    pthread_mutex_t* mut;
    pthread_mutex_t* mut_cac;
    struct thread_data* next;
    int send;
    int cac;
} thread_data;

void* cli_thread(void* cl);

#endif