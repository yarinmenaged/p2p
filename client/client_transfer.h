#ifndef CLIENT_TRANSFER_H
#define CLIENT_TRANSFER_H

int download_chunk(int tracker_fd, int file_id, int chunk_id);
int download_file(int tracker_fd, int file_id);

#endif
