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
#include "network.h"
#include "cache.h"

typedef struct thread_data {
    client* cl;
    client** cl_h;
    pthread_mutex_t* mut;
    pthread_mutex_t* mut_cac;
} thread_data;

void* cli_thread(void* cl);

#endif