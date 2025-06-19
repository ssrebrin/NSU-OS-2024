#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "cache.h"

cache* cache_head = NULL;

void add_to_data(cache* cac, char* buff, int len) {
    if (!cac || !buff || len <= 0) return;

    data* new_node = (data*)malloc(sizeof(data));
    if (!new_node) return;

    new_node->data = (char*)malloc(len);
    if (!new_node->data) {
        free(new_node);
        return;
    }

    memcpy(new_node->data, buff, len);
    new_node->len = len;
    new_node->next = NULL;

    if (!cac->dat) {
        cac->dat = new_node;
    }
    else {
        data* curr = cac->dat;
        while (curr->next) {
            curr = curr->next;
        }
        curr->next = new_node;
    }
}

cache* add_to_cache(char* req, char* host) {

    cache* new_cache = (cache*)malloc(sizeof(cache));
    if (!new_cache) return NULL;

    new_cache->request = strdup(req);
    new_cache->host = strdup(host);
    new_cache->live_time = -1;
    new_cache->birth_time = time(NULL);
    new_cache->working = 0;
    new_cache->dat = NULL;
    new_cache->next = NULL;
    new_cache->status_code = -1;
    cache* t;
    if (!cache_head) {
        cache_head = new_cache;
        //printf("AAAAAAAAAAAAAAAAAAAAAAAAAAAAaa\n");
    }
    else {
        t = cache_head;
        while (t->next) t = t->next;
        t->next = new_cache;
    }

    return new_cache;
}

void get_header_value(const char* headers, const char* name, char* value) {
    size_t name_len = strlen(name);

    const char* p = headers;
    while (*p) {
        const char* line_start = p;
        const char* colon = strchr(line_start, ':');
        if (!colon) break;

        //size_t key_len = colon - line_start;

        if (strncasecmp(line_start, name, name_len) == 0 && line_start[name_len] == ':') {
            const char* val_start = colon + 1;
            while (*val_start == ' ' || *val_start == '\t') val_start++;

            size_t i = 0;
            while (*val_start && *val_start != '\r' && *val_start != '\n' && i < 1024 - 1) {
                value[i++] = *val_start++;
            }
            value[i] = '\0';
            return;
        }

        p = strstr(p, "\n");
        if (!p) break;
        p++;
    }

    value[0] = '\0';
}



int vary_headers_match(const char* req1, const char* req2) {
    char* vary_value = (char*)calloc(1024, 1);
    get_header_value(req1, "Vary", vary_value);
    if (!vary_value[0]) {
        free(vary_value);
        return 1;
    }

    if (strchr(vary_value, '*')) {
        free(vary_value);
        return 1;
    }

    char* v1 = (char*)calloc(1024, 1);
    char* v2 = (char*)calloc(1024, 1);


    char header[128];
    const char* p = vary_value;
    while (*p) {
        while (*p == ' ' || *p == ',') p++;
        if (!*p) break;

        int i = 0;
        while (*p && *p != ',' && *p != '\r' && *p != '\n' && i < sizeof(header) - 1) {
            header[i++] = *p++;
        }
        header[i] = '\0';
        memset(v1, 0, 1024);
        memset(v2, 0, 1024);
        get_header_value(req1, header, v1);
        get_header_value(req2, header, v2);
        if (!v1[0] || !v2[0] || strcmp(v1, v2) != 0) {
            free(v1);
            free(v2);
            free(vary_value);
            return 0;
        }
    }
    free(v1);
    free(v2);
    free(vary_value);
    return 1;
}

void fr_data(data* d) {
    while (d) {
        data* next = d->next;
        if (d->data) free(d->data);
        d->data = NULL;
        free(d);
        d = next;
    }
}

cache* find_cache(char* buf, char* host) {
    //printf(">>>%s\n", buf);
    cache* cur = cache_head;
    while (cur) {
        if (!cur->working && !strcmp(host, cur->host) && vary_headers_match(buf, cur->request)) return cur;
        cur = cur->next;
    }
    printf("{No cache to %s\n", host);
    return NULL;
}

void remove_from_cache(cache* a) {
    if (!a) return;

    cache** p = &cache_head;
    while (*p && *p != a) p = &(*p)->next;
    if (*p) {
        *p = a->next;
        fr_data(a->dat);
        free(a->request);
        free(a->host);
        free(a);
    }
}

void check_live_time_cache() {
    cache* cur = cache_head;
    time_t now = time(NULL);
    while (cur) {
        cache* next = cur->next;
        if ((now - cur->birth_time) > cur->live_time && !cur->working) {
            printf("\nCleared cache:\n==============================================\n%s\n==============================================\n", cur->request);
            remove_from_cache(cur);
        }
        cur = next;
    }
}