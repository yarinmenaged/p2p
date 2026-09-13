#ifndef TRACKER_PEERS_H
#define TRACKER_PEERS_H

#include <pthread.h>
#include "../common.h"

#define MAX_PEERS 100

extern Peer peers[MAX_PEERS];
extern int peer_count;
extern int next_peer_index;
extern pthread_mutex_t peers_mutex;

void remove_peer(int client_fd);
void print_active_peers(void);
void select_chunk_peer(int file_id, int chunk_id, const char *client_ip, int peer_port, ChunkSourceHeader *source_header);

#endif
