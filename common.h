#ifndef COMMON_H
#define COMMON_H

#include <arpa/inet.h>
#include <stdio.h>
#include <string.h>

#define CHUNK_SIZE 1024
#define MAX_CHUNKS 100
#define MAX_FILENAME_LENGTH 300
#define MAX_DIRNAME_LENGTH 300
#define MAX_FILES 100

typedef enum
{
    REQUEST_CHUNK,
    CHUNK_FOUND,
    CHUNK_NOT_FOUND
} P2PMessageType;

typedef struct
{
    int id;
    int size;
    char data[CHUNK_SIZE];
} Chunk;

typedef struct
{
    int file_id;
    char filename[MAX_FILENAME_LENGTH];
    int total_chunks;
    Chunk chunks[MAX_CHUNKS];
    int chunk_count;
} SharedFile;

typedef struct
{
    P2PMessageType type;
    int file_id;
    int chunk_id;
    int size;
} P2PHeader;

typedef struct
{
    char ip[INET_ADDRSTRLEN];
    int port;
} PeerAddress;

typedef enum
{
    SOURCE_PEER,
    SOURCE_SERVER
} ChunkSourceType;

typedef struct
{
    ChunkSourceType type;
    PeerAddress address;
    int total_chunks;
    char filename[MAX_FILENAME_LENGTH];
} ChunkSourceHeader;

typedef struct
{
    int file_id;
    int size;
    int total_chunks;
    char filename[MAX_FILENAME_LENGTH];
} FileInfo;

typedef struct
{
    PeerAddress address;
    SharedFile files[MAX_FILES];
    int file_count;
    int socket_fd;
} Peer;
 
int send_all(int socket_fd, const void *buffer, int size);
int recv_all(int socket_fd, void *buffer, int size);
int recv_until_end(int socket_fd, char *buffer, int size);
SharedFile *get_peer_file(Peer *peer, int file_id);
 
#endif