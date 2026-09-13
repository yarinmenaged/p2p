#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include "client_files.h"

#define BUFFER_SIZE 1024

Peer local_peer;
FileInfo available_files[MAX_FILES];
int available_file_count = 0;

/**************************************************************************
 *!  void parse_file_list(char *buffer)
 **************************************************************************
 *  \brief Parses a newline-separated "FILE ..." listing received from the tracker into available_files.
 *  \param[in] buffer Null-terminated text buffer containing the file listing.
 *  \return None
 **************************************************************************/
void parse_file_list(char *buffer)
{
    char *line;
 
    available_file_count = 0;
 
    line = strtok(buffer, "\n");
 
    while (line != NULL)
    {
        if (available_file_count >= MAX_FILES)
        {
            break;
        }
 
        if (sscanf(line, "FILE %d %s %d %d",
                   &available_files[available_file_count].file_id,
                   available_files[available_file_count].filename,
                   &available_files[available_file_count].size,
                   &available_files[available_file_count].total_chunks) == 4)
        {
            available_file_count++;
        }
 
        line = strtok(NULL, "\n");
    }
} 

/**************************************************************************
 *!  int get_files(int tracker_fd)
 **************************************************************************
 *  \brief Requests the current file listing from the tracker and parses it.
 *  \param[in] tracker_fd Socket descriptor connected to the tracker.
 *  \return 0 on success, -1 on failure.
 **************************************************************************/
int get_files(int tracker_fd)
{
    char buffer[BUFFER_SIZE];
 
    send_all(tracker_fd, "GET_FILES", strlen("GET_FILES"));
 
    if (recv_until_end(tracker_fd, buffer, sizeof(buffer)) < 0)
    {
        return -1;
    }
 
    parse_file_list(buffer);
 
    return 0;
}

/**************************************************************************
 *!  void print_available_files(void)
 **************************************************************************
 *  \brief Prints a formatted table of files known to be available on the tracker.
 *  \return None
 **************************************************************************/
void print_available_files(void)
{
    int i;
    char *filename;

    printf("\n===== Available Files =====\n\n");
    printf("%-5s %-22s %-12s %-10s\n", "ID", "Filename", "Size", "Chunks");
 
    for (i = 0; i < available_file_count; i++)
    {
        filename = strrchr(available_files[i].filename, '/');
        if (filename != NULL)
        {
            filename++;
        }
        else
        {
            filename = available_files[i].filename;
        }

        printf("%-5d %-22s %-12d %-10d\n",
               available_files[i].file_id,
               filename,
               available_files[i].size,
               available_files[i].total_chunks);
    }
 
    printf("\n");
}

/**************************************************************************
 *!  void print_local_files(void)
 **************************************************************************
 *  \brief Prints a formatted table of files and downloaded chunk progress for the local peer.
 *  \return None
 **************************************************************************/
void print_local_files(void)
{
    int i;
    int j;
    int size;
    char *filename;
 
    printf("\n===== Local Files =====\n\n");
    printf("%-5s %-22s %-12s %-12s %-10s\n",
           "ID", "Filename", "Size", "Progress", "Chunks");
 
    for (i = 0; i < local_peer.file_count; i++)
    {
        size = 0;
 
        for (j = 0; j < local_peer.files[i].chunk_count; j++)
        {
            size += local_peer.files[i].chunks[j].size;
        }

        filename = strrchr(local_peer.files[i].filename, '/');
        if (filename != NULL)
        {
            filename++;
        }
        else
        {
            filename = local_peer.files[i].filename;
        }
 
        printf("%-5d %-22s %-12d %d/%d          [",
               local_peer.files[i].file_id,
               filename,
               size,
               local_peer.files[i].chunk_count,
               local_peer.files[i].total_chunks);
 
        for (j = 0; j < local_peer.files[i].chunk_count; j++)
        {
            if (j > 0)
            {
                printf(", ");
            }
 
            printf("%d", local_peer.files[i].chunks[j].id);
        }
 
        printf("]\n");
    }
 
    printf("\n");
}

/**************************************************************************
 *!  Chunk *get_peer_chunk(Peer *peer, int file_id, int chunk_id)
 **************************************************************************
 *  \brief Looks up a specific chunk of a file already held by a peer.
 *  \param[in] peer Peer whose files are searched.
 *  \param[in] file_id ID of the file containing the chunk.
 *  \param[in] chunk_id ID of the chunk to find.
 *  \return Pointer to the matching Chunk, or NULL if not found.
 **************************************************************************/
Chunk *get_peer_chunk(Peer *peer, int file_id, int chunk_id)
{
    int i;
    int j;
 
    for (i = 0; i < peer->file_count; i++)
    {
        if (peer->files[i].file_id != file_id)
        {
            continue;
        }
 
        for (j = 0; j < peer->files[i].chunk_count; j++)
        {
            if (peer->files[i].chunks[j].id == chunk_id)
            {
                return &peer->files[i].chunks[j];
            }
        }
 
        return NULL;
    }
 
    return NULL;
}

/**************************************************************************
 *!  static int merge_file(SharedFile *file, int port)
 **************************************************************************
 *  \brief Writes all downloaded chunks of a file to disk in order, producing the final merged file.
 *  \param[in] file File whose chunks are merged.
 *  \param[in] port Local peer port, used to select the output directory.
 *  \return 0 on success, -1 on failure (e.g. missing chunk or I/O error).
 **************************************************************************/
static int merge_file(SharedFile *file, int port)
{
    FILE *output_file;
    int i;
    int chunk_id;
    char directory[MAX_FILENAME_LENGTH];
    char output_filename[MAX_FILENAME_LENGTH];
    char *filename;
 
    snprintf(directory, sizeof(directory), "client/client_files/%d", port);
 
    if (mkdir("client/client_files", 0777) != 0 && errno != EEXIST)
    {
        perror("mkdir");
        return -1;
    }
 
    if (mkdir(directory, 0777) != 0 && errno != EEXIST)
    {
        perror("mkdir");
        return -1;
    }
 
    filename = strrchr(file->filename, '/');
 
    if (filename != NULL)
    {
        filename++;
    }
    else
    {
        filename = file->filename;
    }
 
    snprintf(output_filename, sizeof(output_filename), "%s/%s", directory, filename);
 
    output_file = fopen(output_filename, "wb");
 
    if (output_file == NULL)
    {
        perror("fopen");
        return -1;
    }
 
    for (chunk_id = 0; chunk_id < file->total_chunks; chunk_id++)
    {
        for (i = 0; i < file->chunk_count; i++)
        {
            if (file->chunks[i].id == chunk_id)
            {
                fwrite(file->chunks[i].data, 1, file->chunks[i].size, output_file);
                break;
            }
        }
 
        if (i == file->chunk_count)
        {
            printf("Chunk %d is missing\n", chunk_id);
            fclose(output_file);
            return -1;
        }
    }
 
    fclose(output_file);
 
    printf("File saved to %s\n", output_filename);
 
    return 0;
}

/**************************************************************************
 *!  int merge_if_complete(int file_id)
 **************************************************************************
 *  \brief Merges a file's chunks to disk once all of its chunks have been downloaded.
 *  \param[in] file_id ID of the file to check and merge.
 *  \return 0 if merged or not yet complete, -1 if the file is unknown or the merge failed.
 **************************************************************************/
int merge_if_complete(int file_id)
{
    SharedFile *file;
 
    file = get_peer_file(&local_peer, file_id);
 
    if (file == NULL)
    {
        return -1;
    }
 
    if (file->chunk_count != file->total_chunks)
    {
        return 0;
    }
 
    printf("File %d is complete. Merging file...\n", file_id);
 
    return merge_file(file, local_peer.address.port);
}
