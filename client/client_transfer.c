#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include "client_transfer.h"
#include "client_files.h"

#define BUFFER_SIZE 1024

static pthread_mutex_t tracker_mutex = PTHREAD_MUTEX_INITIALIZER;

typedef enum
{
    ERROR = -1,
    CHUNK_ALREADY_EXISTS,
    PEER_FOUND,
    PEER_NOT_FOUND
} TrackerResponse;

typedef struct
{
    int tracker_fd;
    int file_id;
    int chunk_id;
} ChunkDownloadArgs;

/**************************************************************************
 *!  static int update_chunk_at_tracker(int file_id, int chunk_id, int tracker_fd)
 **************************************************************************
 *  \brief Notifies the tracker that the local peer now holds a given chunk.
 *  \param[in] file_id ID of the file the chunk belongs to.
 *  \param[in] chunk_id ID of the chunk that was received.
 *  \param[in] tracker_fd Socket descriptor connected to the tracker.
 *  \return 0 on success, -1 on failure.
 **************************************************************************/
static int update_chunk_at_tracker(int file_id, int chunk_id, int tracker_fd)
{
    char request[BUFFER_SIZE];
    int result;
 
    snprintf(request, sizeof(request), "UPDATE_CHUNK %d %d\n", file_id, chunk_id);
 
    pthread_mutex_lock(&tracker_mutex);
    result = send_all(tracker_fd, request, strlen(request));
    pthread_mutex_unlock(&tracker_mutex);
 
    if (result < 0)
    {
        return -1;
    }

    return 0;
}

/**************************************************************************
 *!  static TrackerResponse get_chunk_source_from_tracker(int tracker_fd, int file_id, int chunk_id, PeerAddress *peer, char *filename, int *total_chunks)
 **************************************************************************
 *  \brief Asks the tracker where a chunk can be obtained; if the server is selected, downloads it directly.
 *  \param[in] tracker_fd Socket descriptor connected to the tracker.
 *  \param[in] file_id ID of the file being requested.
 *  \param[in] chunk_id ID of the chunk being requested.
 *  \param[out] peer Filled with the peer address if a peer source is returned.
 *  \param[out] filename Filled with the file's name.
 *  \param[out] total_chunks Filled with the file's total chunk count.
 *  \return A TrackerResponse: PEER_FOUND, PEER_NOT_FOUND, CHUNK_ALREADY_EXISTS, or ERROR.
 **************************************************************************/
static TrackerResponse get_chunk_source_from_tracker(int tracker_fd, int file_id, int chunk_id, PeerAddress *peer, char *filename, int *total_chunks)
{
    int bytes_received;
    char buffer[BUFFER_SIZE];
    char message[BUFFER_SIZE];
    ChunkSourceHeader source_header;
    P2PHeader header;
    char chunk_data[CHUNK_SIZE];
    SharedFile *file;
 
    snprintf(message, sizeof(message), "GET_CHUNK_SOURCE %d %d\n", file_id, chunk_id);
 
    pthread_mutex_lock(&tracker_mutex);
 
    if (send(tracker_fd, message, strlen(message), 0) < 0)
    {
        pthread_mutex_unlock(&tracker_mutex);
        perror("send");
        return ERROR;
    }
 
    memset(buffer, 0, sizeof(buffer));
 
    bytes_received = recv_all(tracker_fd, &source_header, sizeof(source_header));
    if (bytes_received < 0)
    {
        pthread_mutex_unlock(&tracker_mutex);
        return ERROR;
    }

    if (source_header.type == SOURCE_PEER)
    {
        pthread_mutex_unlock(&tracker_mutex);

        *peer = source_header.address;
        strcpy(filename, source_header.filename);
        *total_chunks = source_header.total_chunks;
        return PEER_FOUND;
    }

    else if (source_header.type == SOURCE_SERVER)
    {
        if (recv_all(tracker_fd, &header, sizeof(header)) < 0)
        {
            pthread_mutex_unlock(&tracker_mutex);
            printf("Failed to receive chunk header from server\n");
            return ERROR;
        }
    
        if (header.type != CHUNK_FOUND)
        {
            pthread_mutex_unlock(&tracker_mutex);
            printf("Server could not provide chunk\n");
            return ERROR;
        }
    
        if (header.file_id != file_id || header.chunk_id != chunk_id)
        {
            pthread_mutex_unlock(&tracker_mutex);
            printf("Received unexpected chunk\n");
            return ERROR;
        }
    
        if (header.size <= 0 || header.size > CHUNK_SIZE)
        {
            pthread_mutex_unlock(&tracker_mutex);
            printf("Invalid chunk size\n");
            return ERROR;
        }
    
        if (recv_all(tracker_fd, chunk_data, header.size) < 0)
        {
            pthread_mutex_unlock(&tracker_mutex);
            printf("Failed to receive chunk data from server\n");
            return ERROR;
        }

        pthread_mutex_unlock(&tracker_mutex);

        pthread_mutex_lock(&local_peer_mutex);

        file = get_peer_file(&local_peer, file_id);

        if (file == NULL)
        {
            if (local_peer.file_count >= MAX_FILES)
            {
                pthread_mutex_unlock(&local_peer_mutex);
                printf("Maximum number of files reached\n");
                return ERROR;
            }
        
            file = &local_peer.files[local_peer.file_count];
        
            file->file_id = file_id;
            strcpy(file->filename, source_header.filename);
            file->total_chunks = source_header.total_chunks;
            file->chunk_count = 0;
            local_peer.file_count++;
        
            printf("Added file %d to local peer\n", file_id);
        }

        if (get_peer_chunk(&local_peer, file_id, chunk_id) != NULL)
        {
            pthread_mutex_unlock(&local_peer_mutex);
            return CHUNK_ALREADY_EXISTS;
        }

        file->chunks[file->chunk_count].id = chunk_id;
        file->chunks[file->chunk_count].size = header.size;
        
        memcpy(file->chunks[file->chunk_count].data, chunk_data, header.size);
        
        file->chunk_count++;

        pthread_mutex_unlock(&local_peer_mutex);

        if (update_chunk_at_tracker(file_id, chunk_id, tracker_fd) < 0)
        {
            printf("Failed to update chunk at tracker\n");
            return ERROR;
        }

        printf("Chunk %d of file %d received from server\n", chunk_id, file_id);
        return PEER_NOT_FOUND;
    }
    else
    {
        pthread_mutex_unlock(&tracker_mutex);
        printf("Unknown source type\n");
        return ERROR;
    }
}
 
/**************************************************************************
 *!  static void connect_to_peer(const PeerAddress *peer, int tracker_fd, int file_id, int chunk_id, char *filename, int total_chunks)
 **************************************************************************
 *  \brief Connects to another peer over TCP and requests a specific chunk, storing it locally on success.
 *  \param[in] peer Address of the peer to connect to.
 *  \param[in] tracker_fd Socket descriptor connected to the tracker, used to report the new chunk.
 *  \param[in] file_id ID of the file being requested.
 *  \param[in] chunk_id ID of the chunk being requested.
 *  \param[in] filename Name of the file, used when registering it locally.
 *  \param[in] total_chunks Total chunk count of the file, used when registering it locally.
 *  \return None
 **************************************************************************/
 static void connect_to_peer(const PeerAddress *peer, int tracker_fd, int file_id, int chunk_id, char *filename, int total_chunks)
{
    int peer_fd;
    struct sockaddr_in peer_addr;
    P2PHeader header;
    char chunk_data[CHUNK_SIZE];
    SharedFile *file;
 
    peer_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (peer_fd < 0)
    {
        perror("socket");
        return;
    }
 
    memset(&peer_addr, 0, sizeof(peer_addr));
 
    peer_addr.sin_family = AF_INET;
    peer_addr.sin_port = htons(peer->port);
 
    if (inet_pton(AF_INET, peer->ip, &peer_addr.sin_addr) <= 0)
    {
        perror("inet_pton");
        close(peer_fd);
        return;
    }
 
    if (connect(peer_fd, (struct sockaddr *)&peer_addr, sizeof(peer_addr)) < 0)
    {
        perror("connect");
        close(peer_fd);
        return;
    }
 
    printf("Connected successfully to peer %s:%d\n", peer->ip, peer->port);

    header.type = REQUEST_CHUNK;
    header.file_id = file_id;
    header.chunk_id = chunk_id;
    header.size = 0;
    
    if (send_all(peer_fd, &header, sizeof(header)) < 0)
    {
        close(peer_fd);
        return;
    }

    if (recv_all(peer_fd, &header, sizeof(header)) < 0)
    {
        close(peer_fd);
        return;
    }
    
    if (header.type == CHUNK_FOUND)
    {
        printf("Chunk found on peer: file %d chunk %d, size %d\n", header.file_id, header.chunk_id, header.size);
    
        if (recv_all(peer_fd, chunk_data, header.size) < 0)
        {
            close(peer_fd);
            return;
        }
    
        printf("Received chunk data: %d bytes\n", header.size);

        pthread_mutex_lock(&local_peer_mutex);

        file = get_peer_file(&local_peer, header.file_id);
 
        if (file == NULL)
        {
            if (local_peer.file_count >= MAX_FILES)
            {
                pthread_mutex_unlock(&local_peer_mutex);
                printf("Maximum number of files reached\n");
                close(peer_fd);
                return;
            }
        
            file = &local_peer.files[local_peer.file_count];
        
            file->file_id = header.file_id;
            strcpy(file->filename, filename);
            file->total_chunks = total_chunks;
            file->chunk_count = 0;
        
            local_peer.file_count++;
        
            printf("Added file %d to local peer\n", header.file_id);
        }
        
        if (file->chunk_count >= MAX_CHUNKS)
        {
            pthread_mutex_unlock(&local_peer_mutex);
            printf("Maximum number of chunks reached for file %d\n", header.file_id);
            close(peer_fd);
            return;
        }

        if (get_peer_chunk(&local_peer, header.file_id, header.chunk_id) != NULL)
        {
            pthread_mutex_unlock(&local_peer_mutex);
            printf("Chunk %d of file %d already exists on the current peer.\n", header.chunk_id, header.file_id);
            close(peer_fd);
            return;
        }
        
        file->chunks[file->chunk_count].id = header.chunk_id;
        file->chunks[file->chunk_count].size = header.size;
        
        memcpy(file->chunks[file->chunk_count].data, chunk_data, header.size);
        
        file->chunk_count++;
        
        pthread_mutex_unlock(&local_peer_mutex);

        printf("Chunk %d added to file %d\n", header.chunk_id, header.file_id);

        update_chunk_at_tracker(header.file_id, header.chunk_id, tracker_fd);
 
    }
    else if (header.type == CHUNK_NOT_FOUND)
    {
        printf("Chunk not found on peer: file %d chunk %d\n", header.file_id, header.chunk_id);
    }
 
    close(peer_fd);
}

/**************************************************************************
 *!  int download_chunk(int tracker_fd, int file_id, int chunk_id)
 **************************************************************************
 *  \brief Downloads a single chunk of a file from whichever source the tracker selects, then merges the file if complete.
 *  \param[in] tracker_fd Socket descriptor connected to the tracker.
 *  \param[in] file_id ID of the file to download from.
 *  \param[in] chunk_id ID of the chunk to download.
 *  \return 0 on success or handled failure, 1 on unrecoverable tracker error.
 **************************************************************************/
int download_chunk(int tracker_fd, int file_id, int chunk_id)
{
    PeerAddress chunk_peer;
    char filename[MAX_FILENAME_LENGTH];
    int total_chunks = 0;
    int peer_found;
    int i;

    for (i = 0; i < available_file_count; i++)
    {
        if (available_files[i].file_id == file_id)
        {
            total_chunks = available_files[i].total_chunks;
            break;
        }
    }
    
    if (total_chunks == 0)
    {
        printf("File %d doesn't exist.\n", file_id);
        return 0;
    }
    
    if (chunk_id < 0 || chunk_id >= total_chunks)
    {
        printf("Chunk %d doesn't exist in file %d.\n", chunk_id, file_id);
        return 0;
    }
 
    peer_found = get_chunk_source_from_tracker(tracker_fd, file_id, chunk_id,
                                               &chunk_peer, filename, &total_chunks);
 
    if (peer_found == CHUNK_ALREADY_EXISTS)
    {
        printf("Chunk %d of file %d already exists on the current peer.\n", chunk_id, file_id);
    }
    else if (peer_found == PEER_FOUND)
    {
        printf("Selected peer: %s:%d\n", chunk_peer.ip, chunk_peer.port);
 
        connect_to_peer(&chunk_peer, tracker_fd, file_id, chunk_id, filename, total_chunks);
    }
    else if (peer_found == PEER_NOT_FOUND)
    {
        printf("No peer found for file %d chunk %d. Download directly from the server\n\n", file_id, chunk_id);
    }
    else
    {
        perror("get_chunk_source_from_tracker");
        close(tracker_fd);
        return 1;
    }

    return 0;
}

/**************************************************************************
 *!  static void *download_chunk_thread(void *arg)
 **************************************************************************
 *  \brief Thread entry point that downloads a single chunk and frees its argument bundle.
 *  \param[in] arg Pointer to a heap-allocated ChunkDownloadArgs (freed internally).
 *  \return Always NULL.
 **************************************************************************/
static void *download_chunk_thread(void *arg)
{
    ChunkDownloadArgs *task;

    task = (ChunkDownloadArgs *)arg;

    download_chunk(task->tracker_fd, task->file_id, task->chunk_id);

    free(task);

    return NULL;
}

/**************************************************************************
 *!  int download_file(int tracker_fd, int file_id)
 **************************************************************************
 *  \brief Downloads every missing chunk of a file, creating one thread per chunk so
 *         transfers run in parallel. Waits for all of them to finish before returning.
 *  \param[in] tracker_fd Socket descriptor connected to the tracker.
 *  \param[in] file_id ID of the file to download.
 *  \return 0 on success, -1 if the file is unknown.
 **************************************************************************/
int download_file(int tracker_fd, int file_id)
{
    int i;
    int chunk_id;
    int total_chunks;
    SharedFile *file;
    pthread_t threads[MAX_CHUNKS];
    int thread_started[MAX_CHUNKS];
    ChunkDownloadArgs *task;
 
    total_chunks = -1;
 
    for (i = 0; i < available_file_count; i++)
    {
        if (available_files[i].file_id == file_id)
        {
            total_chunks = available_files[i].total_chunks;
            break;
        }
    }
 
    if (total_chunks == -1)
    {
        printf("File %d not found\n", file_id);
        return -1;
    }
 
    pthread_mutex_lock(&local_peer_mutex);
    file = get_peer_file(&local_peer, file_id);
    if (file != NULL && file->chunk_count == total_chunks)
    {
        pthread_mutex_unlock(&local_peer_mutex);
        printf("File %d already fully downloaded on this client\n", file_id);
        return 0;
    }
    pthread_mutex_unlock(&local_peer_mutex);
 
    printf("Downloading file %d (%d chunks)...\n", file_id, total_chunks);
 
    for (chunk_id = 0; chunk_id < total_chunks; chunk_id++)
    {
        thread_started[chunk_id] = 0;
 
        task = malloc(sizeof(ChunkDownloadArgs));
        if (task == NULL)
        {
            perror("malloc");
            continue;
        }
 
        task->tracker_fd = tracker_fd;
        task->file_id = file_id;
        task->chunk_id = chunk_id;
 
        if (pthread_create(&threads[chunk_id], NULL, download_chunk_thread, task) != 0)
        {
            perror("pthread_create");
            free(task);
            continue;
        }
 
        thread_started[chunk_id] = 1;
    }
 
    for (chunk_id = 0; chunk_id < total_chunks; chunk_id++)
    {
        if (thread_started[chunk_id])
        {
            pthread_join(threads[chunk_id], NULL);
        }
    }

    merge_if_complete(file_id);
 
    printf("All chunks of file %d downloaded successfully\n", file_id);
 
    return 0;
}
