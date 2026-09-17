#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <pthread.h>
#include "../common.h"
#include "tracker_files.h"
#include "tracker_peers.h"

#define BUFFER_SIZE 1024
#define BACKLOG 5

/**************************************************************************
 *!  static void print_tracker_menu(void)
 **************************************************************************
 *  \brief Prints the interactive tracker CLI menu.
 *  \return None
 **************************************************************************/
static void print_tracker_menu(void)
{
    printf("\n===== Tracker Menu =====\n");
    printf("1. Add file\n");
    printf("2. Delete file\n");
    printf("3. Show files\n");
    printf("4. Show active peers\n");
    printf("5. Exit\n");
    printf("Choose an option: ");
    fflush(stdout);
}

/**************************************************************************
 *!  static void *tracker_cli(void *arg)
 **************************************************************************
 *  \brief Runs the tracker's interactive console loop, dispatching menu choices to file/peer management functions.
 *  \param[in] arg Unused thread argument.
 *  \return Always NULL.
 **************************************************************************/
static void *tracker_cli(void *arg)
{
    int choice;
    char filename[MAX_FILENAME_LENGTH];
    int size;
    (void)arg;
 
    while (1)
    {
        print_tracker_menu();
 
        if (scanf("%d", &choice) != 1)
        {
            printf("Invalid input\n");
            while (getchar() != '\n')
            {
            }
            continue;
        }
        printf("\n");
 
        if (choice == 1)
        {
            printf("Enter filename: ");
            scanf("%s", filename);
 
            printf("Enter size: ");
            scanf("%d", &size);
 
            add_server_file(filename, size);
        }
        else if (choice == 2)
        {
            print_server_files();

            printf("Enter file ID to delete: ");
            int file_id;
            scanf("%d", &file_id);
            delete_server_file(file_id);
        }
        else if (choice == 3)
        {
            print_server_files();
        }
        else if (choice == 4)
        {
            print_active_peers();
        }
        else if (choice == 5)
        {
            printf("Exiting tracker\n");
            exit(0);
        }
        else
        {
            printf("Invalid option\n");
        }
    }
 
    return NULL;
}

/**************************************************************************
 *!  static int recv_command(int client_fd, char *out, int out_size, char *pending, int *pending_len, int pending_cap)
 **************************************************************************
 *  \brief Reads one newline-terminated command from client_fd. TCP has no message
 *         boundaries, so a single recv() can contain part of a command, a whole
 *         command, or several; leftover bytes are kept in `pending` across calls.
 *  \param[in] client_fd Socket descriptor to read from.
 *  \param[out] out Buffer that receives the null-terminated command, without the newline.
 *  \param[in] out_size Size of out.
 *  \param[in,out] pending Buffer holding bytes already read but not yet consumed.
 *  \param[in,out] pending_len Number of valid bytes currently stored in pending.
 *  \param[in] pending_cap Capacity of pending.
 *  \return Length of the command on success, 0 on clean disconnect, -1 on error.
 **************************************************************************/
static int recv_command(int client_fd, char *out, int out_size, char *pending, int *pending_len, int pending_cap)
{
    char *newline;
    int idx;
    int remaining;
    int bytes_received;
    int line_len;

    while (1)
    {
        newline = memchr(pending, '\n', *pending_len);

        if (newline != NULL)
        {
            idx = (int)(newline - pending);
            line_len = (idx < out_size - 1) ? idx : out_size - 1;

            memcpy(out, pending, line_len);
            out[line_len] = '\0';

            remaining = *pending_len - idx - 1;
            memmove(pending, pending + idx + 1, remaining);
            *pending_len = remaining;

            return line_len;
        }

        if (*pending_len >= pending_cap)
        {
            /* Command too long for the buffer. drop it so we don't get stuck. */
            *pending_len = 0;
        }

        bytes_received = recv(client_fd, pending + *pending_len, pending_cap - *pending_len, 0);

        if (bytes_received <= 0)
        {
            return bytes_received;
        }

        *pending_len += bytes_received;
    }
}

/**************************************************************************
 *!  static void *handle_client(void *arg)
 **************************************************************************
 *  \brief Per-connection thread that parses tracker protocol commands from a peer/client and responds.
 *  \param[in] arg Pointer to the heap-allocated client socket descriptor (freed internally).
 *  \return Always NULL.
 **************************************************************************/
static void *handle_client(void *arg)
{
    int client_fd;
    int bytes_received;
    int peer_port = -1;
    char buffer[BUFFER_SIZE];
    char pending[BUFFER_SIZE * 2];
    int pending_len = 0;
    char client_ip[INET_ADDRSTRLEN];
    char response[BUFFER_SIZE];
    struct sockaddr_in client_addr;
    socklen_t client_len;
    Peer *peer;
    char *token;
    int file_id;
    int chunk_id;
    SharedFile *file;
    ChunkSourceHeader source_header;
    ServerFile *server_file = NULL;
    int i;
    int offset;
 
    client_fd = *(int *)arg;
    free(arg);
 
    client_len = sizeof(client_addr);
 
    if (getpeername(client_fd, (struct sockaddr *)&client_addr, &client_len) < 0)
    {
        perror("getpeername");
        close(client_fd);
        return NULL;
    }
 
    if (inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, sizeof(client_ip)) == NULL)
    {
        perror("inet_ntop");
        close(client_fd);
        return NULL;
    }
 
    while (1)
    {
        memset(buffer, 0, sizeof(buffer));
 
        bytes_received = recv_command(client_fd, buffer, sizeof(buffer), pending, &pending_len, sizeof(pending));
        if (bytes_received < 0)
        {
            perror("recv");
            break;
        }
 
        if (bytes_received == 0)
        {
            printf("Client disconnected.\n");
            break;
        }
 
        if (peer_port > 0)
        {
            printf("\nReceived from %s:%d: %s\n", client_ip, peer_port, buffer);
        }
        else
        {
            printf("\nReceived from %s: %s\n", client_ip, buffer);
        }
 
        if (sscanf(buffer, "REGISTER %d", &peer_port) == 1)
        {
            pthread_mutex_lock(&peers_mutex);
 
            if (peer_count >= MAX_PEERS)
            {
                pthread_mutex_unlock(&peers_mutex);
                snprintf(response, sizeof(response), "REGISTER_FAILED");
            }
            else
            {
                peer = &peers[peer_count];
 
                strncpy(peer->address.ip, client_ip, sizeof(peer->address.ip) - 1);
                peer->address.ip[sizeof(peer->address.ip) - 1] = '\0';
                peer->address.port = peer_port;
                peer->socket_fd = client_fd;
 
                peer_count++;
 
                pthread_mutex_unlock(&peers_mutex);
 
                printf("Registered peer %s:%d\n", client_ip, peer_port);

                offset = 0;
                offset += snprintf(response + offset, sizeof(response) - offset, "REGISTER_OK\n");
                
                for (i = 0; i < server_file_count; i++)
                {
                    offset += snprintf(response + offset, sizeof(response) - offset,
                                    "FILE %d %s %d %d\n",
                                    server_files[i].file_id,
                                    server_files[i].filename,
                                    server_files[i].size,
                                    server_files[i].total_chunks);
                }
                
                snprintf(response + offset, sizeof(response) - offset, "END\n");
            }
        }

        else if (strcmp(buffer, "GET_FILES") == 0)
        {        
            offset = 0;

            for (i = 0; i < server_file_count; i++)
            {
                offset += snprintf(response + offset, sizeof(response) - offset,
                                "FILE %d %s %d %d\n",
                                server_files[i].file_id,
                                server_files[i].filename,
                                server_files[i].size,
                                server_files[i].total_chunks);
            }
        
            snprintf(response + offset, sizeof(response) - offset, "END\n");
        }

        else if (strncmp(buffer, "GET_CHUNK_SOURCE ", 17) == 0)
        {
            token = strtok(buffer, " ");
            token = strtok(NULL, " ");  
            file_id = atoi(token);
            token = strtok(NULL, " ");
            chunk_id = atoi(token);
        
            select_chunk_peer(file_id, chunk_id, client_ip, peer_port, &source_header);
            
            for (i = 0; i < server_file_count; i++)
            {
                if (server_files[i].file_id == file_id)
                {
                    server_file = &server_files[i];
                    source_header.total_chunks = server_files[i].total_chunks;
                    strcpy(source_header.filename, server_files[i].filename);
                }
            }

            if (server_file == NULL)
            {
                printf("File %d not found on server\n", file_id);
                continue;
            }

            if (send_all(client_fd, &source_header, sizeof(source_header)) < 0)
            {
                printf("Failed to send chunk source\n");
                continue;
            }


            if (source_header.type == SOURCE_SERVER)
            {
                if (send_chunk_from_server(client_fd, file_id, chunk_id) < 0)
                {
                    printf("Failed to send chunk from server\n");
                    continue;
                }
            }

            print_tracker_menu();
            printf("\n");
            continue;
        }

        else if (strncmp(buffer, "UPDATE_CHUNK", 12) == 0)
        {
            if (sscanf(buffer, "UPDATE_CHUNK %d %d", &file_id, &chunk_id) == 2)
            {
                file = get_peer_file(peer, file_id);
        
                if (file == NULL)
                {
                    if (peer->file_count >= MAX_FILES)
                    {
                        printf("Maximum number of files reached for peer\n");
                    }
                    else
                    {
                        for (i = 0; i < server_file_count; i++)
                        {
                            if (server_files[i].file_id == file_id)
                            {
                                break;
                            }
                        }
                
                        if (i == server_file_count)
                        {
                            printf("File %d does not exist on server\n", file_id);
                        }
                        else
                        {
                            file = &peer->files[peer->file_count];
                
                            file->file_id = file_id;
                            strcpy(file->filename, server_files[i].filename);
                            file->total_chunks = server_files[i].total_chunks;
                            file->chunk_count = 0;
                
                            peer->file_count++;
                
                            printf("File %d added to peer\n", file_id);
                
                            file->chunks[file->chunk_count].id = chunk_id;
                            file->chunk_count++;
                
                            printf("Peer updated: file %d chunk %d\n", file_id, chunk_id);
                        }
                    }
                }

                else if (file->chunk_count >= MAX_CHUNKS)
                {
                    printf("Maximum number of chunks reached for file %d\n", file_id);
                }

                else
                {
                    file->chunks[file->chunk_count].id = chunk_id;
                    file->chunk_count++;
                
                    printf("Peer updated: file %d chunk %d\n", file_id, chunk_id);
                }
            }
            print_tracker_menu();
            printf("\n");
            continue;
        }

        else if (strcmp(buffer, "DISCONNECT") == 0)
        {
            remove_peer(client_fd);
            close(client_fd);
            print_tracker_menu();
            printf("\n");
            break;
        }
        else
        {
            snprintf(response, sizeof(response), "UNKNOWN_COMMAND");
        }
 
        if (send_all(client_fd, response, strlen(response)) < 0)
        {
            perror("send");
            break;
        }
        print_tracker_menu();
        printf("\n");
    }
 
    close(client_fd);
 
    return NULL;
}
 
/**************************************************************************
 *!  int main(int argc, char *argv[])
 **************************************************************************
 *  \brief Entry point; loads server files, starts the listening socket and CLI thread, and accepts client connections.
 *  \param[in] argc Number of command-line arguments.
 *  \param[in] argv Command-line arguments; argv[1] is the listening port.
 *  \return 0 on normal exit, 1 on startup failure.
 **************************************************************************/
int main(int argc, char *argv[])
{
    int port;
    int server_fd;
    int client_fd;
    int *client_fd_ptr;
    struct sockaddr_in server_addr;
    struct sockaddr_in client_addr;
    socklen_t client_len;
    pthread_t thread_id;
    pthread_t cli_thread;
 
    if (argc != 2)
    {
        printf("Usage: %s <port>\n", argv[0]);
        return 1;
    }

    init_server_files();
    update_server_file_metadata();
 
    port = atoi(argv[1]);
 
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0)
    {
        perror("socket");
        return 1;
    }
 
    memset(&server_addr, 0, sizeof(server_addr));
 
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port);
 
    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
    {
        perror("bind");
        close(server_fd);
        return 1;
    }
 
    if (listen(server_fd, BACKLOG) < 0)
    {
        perror("listen");
        close(server_fd);
        return 1;
    }
 
    printf("Tracker is listening on port %d...\n", port);

    print_server_files();
 
    pthread_create(&cli_thread, NULL, tracker_cli, NULL);
 
    while (1)
    {
        client_len = sizeof(client_addr);
 
        client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
        if (client_fd < 0)
        {
            perror("accept");
            continue;
        }
 
        printf("\nClient connected!\n");
 
        client_fd_ptr = malloc(sizeof(int));
        if (client_fd_ptr == NULL)
        {
            perror("malloc");
            close(client_fd);
            continue;
        }
 
        *client_fd_ptr = client_fd;
 
        /* Each client runs in its own thread so the accept loop is never blocked. */
        if (pthread_create(&thread_id, NULL, handle_client, client_fd_ptr) != 0)
        {
            perror("pthread_create");
            free(client_fd_ptr);
            close(client_fd);
            continue;
        }
 
        pthread_detach(thread_id);
    }
 
    close(server_fd);
 
    return 0;
}
