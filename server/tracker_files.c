#include <stdio.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>
#include "tracker_files.h"
#include "../common.h"

ServerFile server_files[MAX_FILES];
int server_file_count = 0;

/**************************************************************************
 *!  int delete_server_file(int file_id)
 **************************************************************************
 *  \brief Removes a server file from disk and from the in-memory registry.
 *  \param[in] file_id ID of the file to delete.
 *  \return 0 on success, -1 if the file wasn't found or couldn't be removed.
 **************************************************************************/
int delete_server_file(int file_id)
{
    int i;
    int j;

    for (i = 0; i < server_file_count; i++)
    {
        if (server_files[i].file_id != file_id)
        {
            continue;
        }

        if (remove(server_files[i].filename) != 0)
        {
            perror("remove");
            return -1;
        }

        for (j = i; j < server_file_count - 1; j++)
        {
            server_files[j] = server_files[j + 1];
        }

        server_file_count--;

        printf("File %d deleted successfully\n", file_id);

        return 0;
    }

    printf("File %d not found\n", file_id);

    return -1;
}

/**************************************************************************
 *!  void print_server_files(void)
 **************************************************************************
 *  \brief Prints a formatted table of all files registered on the server.
 *  \return None
 **************************************************************************/
void print_server_files(void)
{
    int i;
    char *filename;

    printf("\n===== Server Files =====\n\n");
    printf("%-5s %-22s %-12s %-10s\n", "ID", "Filename", "Size", "Chunks");

    for (i = 0; i < server_file_count; i++)
    {
        filename = strrchr(server_files[i].filename, '/');
        if (filename != NULL)
        {
            filename++;
        }
        else
        {
            filename = server_files[i].filename;
        }

        printf("%-5d %-22s %-12d %-10d\n",
               server_files[i].file_id,
               filename,
               server_files[i].size,
               server_files[i].total_chunks);
    }

    printf("\n");
}

/**************************************************************************
 *!  int add_server_file(const char *filename, int size)
 **************************************************************************
 *  \brief Creates a placeholder file of the given size under server_files and registers it.
 *  \param[in] filename Name of the file to create.
 *  \param[in] size Size in bytes of the file to generate.
 *  \return 0 on success, -1 on failure.
 **************************************************************************/
int add_server_file(const char *filename, int size)
{
    FILE *file;
    int i;
    int file_id;

    if (server_file_count >= MAX_FILES)
    {
        printf("Maximum number of server files reached\n");
        return -1;
    }

    file_id = server_file_count + 1;

    snprintf(server_files[server_file_count].filename, MAX_FILENAME_LENGTH, "server/server_files/%s", filename);

    server_files[server_file_count].file_id = file_id;

    file = fopen(server_files[server_file_count].filename, "wb");

    if (file == NULL)
    {
        perror("fopen");
        return -1;
    }

    for (i = 0; i < size; i++)
    {
        fputc('A' + server_file_count, file);
    }

    fclose(file);

    server_files[server_file_count].size = size;
    server_files[server_file_count].total_chunks = (size + CHUNK_SIZE - 1) / CHUNK_SIZE;

    printf("File added successfully: ID=%d, filename=%s, size=%d, chunks=%d\n",
        file_id,
        server_files[server_file_count].filename,
        size,
        server_files[server_file_count].total_chunks);

    server_file_count++;

    return 0;
}

/**************************************************************************
 *!  int send_chunk_from_server(int client_fd, int file_id, int chunk_id)
 **************************************************************************
 *  \brief Reads a chunk of a server file and sends it to a connected client.
 *  \param[in] client_fd Socket descriptor of the requesting client.
 *  \param[in] file_id ID of the file the chunk belongs to.
 *  \param[in] chunk_id Index of the chunk to send.
 *  \return 0 on success, -1 on failure.
 **************************************************************************/
int send_chunk_from_server(int client_fd, int file_id, int chunk_id)
{
    int i;
    int offset;
    int size;
    FILE *file;
    char chunk_data[CHUNK_SIZE];
    P2PHeader header;

    for (i = 0; i < server_file_count; i++)
    {
        if (server_files[i].file_id == file_id)
        {
            break;
        }
    }

    if (i == server_file_count)
    {
        return -1;
    }

    if (chunk_id < 0 || chunk_id >= server_files[i].total_chunks)
    {
        return -1;
    }

    file = fopen(server_files[i].filename, "rb");

    if (file == NULL)
    {
        perror("fopen");
        return -1;
    }

    offset = chunk_id * CHUNK_SIZE;

    if (fseek(file, offset, SEEK_SET) != 0)
    {
        fclose(file);
        return -1;
    }

    size = fread(chunk_data, 1, CHUNK_SIZE, file);

    if (size <= 0)
    {
        fclose(file);
        return -1;
    }

    fclose(file);

    header.type = CHUNK_FOUND;
    header.file_id = file_id;
    header.chunk_id = chunk_id;
    header.size = size;

    if (send_all(client_fd, &header, sizeof(header)) < 0)
    {
        return -1;
    }

    if (send_all(client_fd, chunk_data, size) < 0)
    {
        return -1;
    }

    printf("Server sent file %d chunk %d (%d bytes)\n", file_id, chunk_id, size);

    return 0;
}

/**************************************************************************
 *!  void init_server_files(void)
 **************************************************************************
 *  \brief Scans the server_files directory and rebuilds the in-memory file registry from disk.
 *  \return None
 **************************************************************************/
void init_server_files(void)
{
    DIR *directory;
    struct dirent *entry;
    struct stat file_stat;
    char path[MAX_FILENAME_LENGTH];

    directory = opendir("server/server_files");

    if (directory == NULL)
    {
        perror("opendir");
        return;
    }

    server_file_count = 0;

    while ((entry = readdir(directory)) != NULL)
    {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
        {
            continue;
        }

        if (server_file_count >= MAX_FILES)
        {
            printf("Maximum number of server files reached\n");
            break;
        }

        snprintf(path, sizeof(path), "server/server_files/%s", entry->d_name);

        if (stat(path, &file_stat) != 0)
        {
            perror("stat");
            continue;
        }

        if (!S_ISREG(file_stat.st_mode))
        {
            continue;
        }

        strcpy(server_files[server_file_count].filename, path);
        server_files[server_file_count].file_id = server_file_count + 1;
        server_files[server_file_count].size = file_stat.st_size;
        server_files[server_file_count].total_chunks =
            (server_files[server_file_count].size + CHUNK_SIZE - 1) / CHUNK_SIZE;

        server_file_count++;
    }

    closedir(directory);
}

/**************************************************************************
 *!  void update_server_file_metadata(void)
 **************************************************************************
 *  \brief Refreshes size and chunk-count metadata for every registered server file from disk.
 *  \return None
 **************************************************************************/
void update_server_file_metadata(void)
{
    int i;
    FILE *file;

    for (i = 0; i < server_file_count; i++)
    {
        file = fopen(server_files[i].filename, "rb");

        if (file == NULL)
        {
            perror("fopen");
            continue;
        }

        fseek(file, 0, SEEK_END);
        server_files[i].size = ftell(file);
        server_files[i].total_chunks = (server_files[i].size + CHUNK_SIZE - 1) / CHUNK_SIZE;

        fclose(file);
    }
    printf("\n");
}
