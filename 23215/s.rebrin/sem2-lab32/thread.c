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


#define STAY_CONNECTION_IN_WRITING_FROM_BUFFER 1
#define CLIENT_TIMEOUT 10

int minn(int a, int b) {
	return a < b ? a : b;
}

client* clear_connection(client* cur, client** hd, pthread_mutex_t* mut) {
	pthread_mutex_lock(mut);
	client* next = cur->next;
	close(cur->cli_fd);
	if (cur->inet_fd > 0) close(cur->inet_fd);
	client** p = hd;
	while (*p && *p != cur) p = &(*p)->next;
	if (*p) *p = next;
	free(cur->host);

	if (cur->headers_collectors)
		free(cur->headers_collectors);
	cur->headers_collectors = NULL;
	free(cur);
	pthread_mutex_unlock(mut);
	return next;
}

void* cli_thread(void* cl) {
	char buffer[BUFFER_SIZE];
	char hst[1024];
	client* cli = ((thread_data*)cl)->cl;
	client** hd = ((thread_data*)cl)->cl_h;
	pthread_mutex_t* mut = ((thread_data*)cl)->mut;
	pthread_mutex_t* mut_cac = ((thread_data*)cl)->mut_cac;


	struct timeval tv;
	tv.tv_sec = 5;
	tv.tv_usec = 0;
	fd_set read_fds, write_fds;

	while (1) {
		if (time(NULL) - cli->last_activity > CLIENT_TIMEOUT) {
			printf("\tClient %d timed out\n", cli->cli_fd);
			cli = clear_connection(cli, hd, mut);
			break;
		}

		int max_fd = cli->cli_fd + cli->inet_fd - minn(cli->cli_fd, cli->inet_fd);
		FD_ZERO(&read_fds);
		FD_ZERO(&write_fds);
		FD_SET(cli->cli_fd, &read_fds);
		FD_SET(cli->cli_fd, &write_fds);
		if (cli->inet_fd > 0) FD_SET(cli->inet_fd, &read_fds);
		if (cli->cli_fd > max_fd) max_fd = cli->cli_fd;
		if (cli->inet_fd > max_fd) max_fd = cli->inet_fd;


		int activity = select(max_fd + 1, &read_fds, &write_fds, NULL, &tv);
		//logs();
		if (activity < 0) {
			perror("select");
			exit(1);
		}
		if (activity == 0) {
			pthread_mutex_lock(mut_cac);
			check_live_time_cache();
			pthread_mutex_unlock(mut_cac);
			break;
		}


		if (FD_ISSET(cli->cli_fd, &read_fds) && !cli->tunneling) {
			int r = read(cli->cli_fd, buffer, BUFFER_SIZE - 1);
			if (r <= 0) {
				if (cli->len != -1 && cli->tot >= cli->len + cli->headers_len) {
					if (cli->cur_cache) {
						pthread_mutex_lock(mut_cac);
						cli->cur_cache->working = 0;
						pthread_mutex_unlock(mut_cac);
					}
					//printf("here\n");
					close(cli->inet_fd);
					cli->inet_fd = 0;
					cli->writing = 0;
					printf("Done cache\n");
				}
				printf("\tClient %d disconnected\n", cli->cli_fd);
				cli = clear_connection(cli, hd, mut);
				break;
			}

			buffer[r] = '\0';
			cli->last_activity = time(NULL);

			if (!cli->writing) {
				cli->writing = 1;
				cli->tot = 0;
				cli->len = -1;
				cli->header_len = 0;
				cli->headers_len = 0;
				cli->writing_to_client = 0;
				cli->writing_to_client_total = 0;
				cli->caching = 0;

				char method[16], path[256], version[16];
				sscanf(buffer, "%15s %255s %15s", method, path, version);
				if (strcmp(method, "CONNECT") == 0) {
					cli = clear_connection(cli, hd, mut);
					break;
					printf("\tCONNECT to %s\n", path);
					parse_http_request(buffer, hst);
					if (hst[0] != '\0' && (cli->host[0] == '\0' || strcmp(hst, cli->host))) {
						if (cli->inet_fd > 0) {
							close(cli->inet_fd);
							cli->inet_fd = 0;
						}
						if (con_to_host(&cli->inet_fd, hst)) {
							cli->inet_fd = 0;
							cli = clear_connection(cli, hd, mut);
							break;
						}
						else {
							strncpy(cli->host, hst, BUFFER_SIZE - 1);
							cli->host[BUFFER_SIZE - 1] = '\0';
						}
					}

					const char* response = "HTTP/1.1 200 Connection Established\r\n\r\n";
					if (write(cli->cli_fd, response, strlen(response)) < 0) {
						printf("\tError CONNECT request client\n");
						cli = clear_connection(cli, hd, mut);
						break;
					}

					cli->tunneling = 1;
					cli->using_cache = 0;
					cli->caching = 0;
					cli->collect_headers = -1;
				}
				else if (strcmp(method, "GET") == 0) {
					cli->caching = 1;
					parse_http_request(buffer, hst);
					pthread_mutex_lock(mut_cac);
					cache* pot_cache = find_cache(buffer, hst);
					if (pot_cache) {
						printf("-----Using prepeared cache\n");
						cli->cur_cache = pot_cache;
						cli->cur_data = pot_cache->dat;
						cli->using_cache = 1;
						pthread_mutex_unlock(mut_cac);
					}
					else {
						cli->using_cache = 0;
						cli->cur_cache = add_to_cache(buffer, hst);
						cli->cur_cache->working = 1;
						pthread_mutex_unlock(mut_cac);
						if (hst[0] != '\0' && (cli->host[0] == '\0' || strcmp(hst, cli->host))) {
							if (cli->inet_fd > 0) {
								close(cli->inet_fd);
								cli->inet_fd = 0;
							}

							if (con_to_host(&cli->inet_fd, hst)) {

								cli->inet_fd = 0;
								cli = clear_connection(cli, hd, mut);
								break;
							}
							else {
								strncpy(cli->host, hst, BUFFER_SIZE - 1);
								cli->host[BUFFER_SIZE - 1] = '\0';
								printf("[%s]\n", cli->host);
							}
							printf(">>New req %s to %d\n", cli->host, cli->cli_fd);
						}
						if ((r = write(cli->inet_fd, buffer, strlen(buffer))) < 0) {
							printf("\tWriting to socket error\n");
							cli = clear_connection(cli, hd, mut);
							break;
						}
					}
				}
				else {
					cli = clear_connection(cli, hd, mut);
					break;
				}
			}
		}

		// Tunneling processing
		if (cli->tunneling) {
			if (FD_ISSET(cli->cli_fd, &read_fds)) {
				int r = read(cli->cli_fd, buffer, BUFFER_SIZE);
				if (r <= 0) {
					printf("\tClient %d disconnected in tunneling\n", cli->cli_fd);
					cli = clear_connection(cli, hd, mut);
					break;
				}
				int written = 0;
				while (written < r) {
					int w = write(cli->inet_fd, buffer + written, r - written);
					if (w < 0) {
						printf("\tWriting to server in tunneling error\n");
						cli = clear_connection(cli, hd, mut);
						break;
					}
					written += w;
				}
				cli->last_activity = time(NULL);
			}
			if (cli->inet_fd && FD_ISSET(cli->inet_fd, &read_fds)) {
				int r = read(cli->inet_fd, buffer, BUFFER_SIZE);
				if (r <= 0) {
					printf("\tServer %s disconnected in tunneling\n", cli->host);
					cli = clear_connection(cli, hd, mut);
					break;
				}
				int written = 0;
				while (written < r) {
					int w = write(cli->cli_fd, buffer + written, r - written);
					if (w < 0) {
						printf("\tWriting to client error\n");
						cli = clear_connection(cli, hd, mut);
						break;
					}
					written += w;
				}
				cli->last_activity = time(NULL);
			}
		}
		else {
			// For slow client
			if (FD_ISSET(cli->cli_fd, &write_fds) && cli->writing_to_client) {
				cli->writing_to_client += write(cli->cli_fd, cli->buffer + cli->writing_to_client, cli->writing_to_client_total - cli->writing_to_client);
				if (cli->writing_to_client == cli->writing_to_client_total) cli->writing_to_client = 0;
				if (STAY_CONNECTION_IN_WRITING_FROM_BUFFER) cli->last_activity = time(NULL);
				printf("+++++++++++++++++Used\n");
			}
			//if (FD_ISSET(cli->cli_fd, &write_fds)) printf("fdfsdfasdfasdfasfasfdfsdfSDF\n");
			// Cache processing
			if (cli->using_cache && FD_ISSET(cli->cli_fd, &write_fds) && !cli->writing_to_client && cli->cur_data && cli->writing) {
				memcpy(cli->buffer, cli->cur_data->data, cli->cur_data->len);
				cli->writing_to_client = write(cli->cli_fd, cli->buffer, cli->cur_data->len);
				cli->writing_to_client_total = cli->cur_data->len;
				if (cli->writing_to_client == cli->cur_data->len) cli->writing_to_client = 0;
				cli->last_activity = time(NULL);
				cli->cur_data = cli->cur_data->next;
				if (!cli->cur_data) cli->writing = 0;
			}
			else if (cli->inet_fd && FD_ISSET(cli->cli_fd, &write_fds) && FD_ISSET(cli->inet_fd, &read_fds) && !cli->writing_to_client && (cli->tot <= cli->len + cli->headers_len || cli->len == -1)) {
				int bytes_read = read(cli->inet_fd, cli->buffer, BUFFER_SIZE - 1);
				if (bytes_read < 0) {
					printf("\tReading from socket error\n");
					cli = clear_connection(cli, hd, mut);
					break;
				}
				if (bytes_read == 0) {
					if (cli->cur_cache) {
						pthread_mutex_lock(mut_cac);
						cli->cur_cache->working = 0;
						printf("{Done cache\n");
						pthread_mutex_unlock(mut_cac);
					}
					close(cli->inet_fd);
					cli->inet_fd = 0;
					cli->writing = 0;
					continue;
				}

				// Headers parsing
				if (cli->collect_headers != -1) {
					strncpy(cli->headers_collectors + cli->collect_headers, cli->buffer, minn(4096, cli->collect_headers + bytes_read));
					if (strstr(cli->headers_collectors, "\r\n\r\n")) {
						if (!cli->len || cli->cur_cache->live_time == -1 || cli->cur_cache->status_code == -1) {
							int len, live, status;
							pthread_mutex_lock(mut_cac);
							parse_headers(cli->buffer, &len, &live, &status);
							if (!cli->len) cli->len = len == -1 ? cli->len : len;
							if (cli->cur_cache->live_time == -1) cli->cur_cache->live_time = live == -1 ? cli->cur_cache->live_time : 60;
							if (cli->cur_cache->status_code == -1) cli->cur_cache->status_code = status == -1 ? cli->cur_cache->status_code : status;
							if ((status != -1 && status / 100 != 2) || len == -1) {
								remove_from_cache(cli->cur_cache);
								cli->cur_cache = NULL;
								printf("Stop caching ");
								if (len == -1) printf("no length ");
								if (status / 100 != 2) printf("bad status - %d ", status);
								printf("\n");
							}
							pthread_mutex_unlock(mut_cac);
						}
						if (!cli->headers_len) {
							char* header_end = strstr(cli->buffer, "\r\n\r\n");
							if (header_end) cli->headers_len = cli->tot + (header_end - cli->buffer) + 4;
						}
						if (cli->headers_collectors)
							free(cli->headers_collectors);
						cli->headers_collectors = NULL;
						cli->collect_headers = -1;
					}
				}

				// Getting data
				cli->buffer[bytes_read] = '\0';
				cli->writing_to_client = write(cli->cli_fd, cli->buffer, bytes_read);
				cli->writing_to_client_total = bytes_read;
				printf("\tI get %d: %d bytes from %d, and send %d bytes\n", bytes_read, cli->tot + bytes_read, cli->len + cli->headers_len, cli->writing_to_client);
				if (cli->writing_to_client >= bytes_read) cli->writing_to_client = 0;
				cli->tot += bytes_read;
				pthread_mutex_lock(mut_cac);
				if (cli->cur_cache && cli->cur_cache->working) {
					add_to_data(cli->cur_cache, cli->buffer, bytes_read);
				}
				pthread_mutex_unlock(mut_cac);
				if (cli->len != -1 && cli->tot >= cli->len + cli->headers_len) {
					if (cli->cur_cache) {
						pthread_mutex_lock(mut_cac);
						cli->cur_cache->working = 0;
						pthread_mutex_unlock(mut_cac);
					}
					//printf("here\n");
					close(cli->inet_fd);
					cli->inet_fd = 0;
					cli->writing = 0;
					printf("Done cache\n");
				}
				cli->last_activity = time(NULL);
			}
		}
	}
	return NULL;
}