#ifndef TRACKER_FILES_H
#define TRACKER_FILES_H

#include "../common.h"

typedef struct
{
    int file_id;
    char filename[MAX_FILENAME_LENGTH];
    int size;
    int total_chunks;
} ServerFile;

extern ServerFile server_files[MAX_FILES];
extern int server_file_count;

int add_server_file(const char *filename, int size);
int delete_server_file(int file_id);
void print_server_files(void);
void init_server_files(void);
void update_server_file_metadata(void);
int send_chunk_from_server(int client_fd, int file_id, int chunk_id);

#endif
