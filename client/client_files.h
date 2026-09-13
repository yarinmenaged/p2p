#ifndef CLIENT_FILES_H
#define CLIENT_FILES_H

#include "../common.h"

extern Peer local_peer;
extern FileInfo available_files[MAX_FILES];
extern int available_file_count;

void parse_file_list(char *buffer);
int get_files(int tracker_fd);
void print_available_files(void);
void print_local_files(void);
Chunk *get_peer_chunk(Peer *peer, int file_id, int chunk_id);
int merge_if_complete(int file_id);

#endif
