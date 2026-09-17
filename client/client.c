#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <pthread.h>
#include "../common.h"
#include "client_files.h"
#include "client_transfer.h"

#define BUFFER_SIZE 1024

/**************************************************************************
 *!  static void print_client_menu(void)
 **************************************************************************
 *  \brief Prints the interactive client CLI menu.
 *  \return None
 **************************************************************************/
static void print_client_menu(void)
{
    printf("\n===== P2P Client =====\n");
    printf("1. Download file\n");
    printf("2. Download chunk\n");
    printf("3. Show server files\n");
    printf("4. Show my files\n");
    printf("5. Exit\n\n");
    printf("Choose an option: ");
    fflush(stdout);
}
 
/**************************************************************************
 *!  static void *handle_peer(void *arg)
 **************************************************************************
 *  \brief Per-connection thread that serves a single chunk request from another peer.
 *  \param[in] arg Pointer to the heap-allocated peer socket descriptor (freed internally).
 *  \return Always NULL.
 **************************************************************************/
static void *handle_peer(void *arg)
{
    int client_fd;
    Chunk *chunk;
    P2PHeader header;

    client_fd = *(int *)arg;
    free(arg);

    if (recv_all(client_fd, &header, sizeof(header)) < 0)
    {
        close(client_fd);
        return NULL;
    }

    if (header.type == REQUEST_CHUNK)
    {
        printf("Received request for file %d chunk %d\n", header.file_id, header.chunk_id);
        
        pthread_mutex_lock(&local_peer_mutex);
        chunk = get_peer_chunk(&local_peer, header.file_id, header.chunk_id);
        pthread_mutex_unlock(&local_peer_mutex);
 
        if (chunk != NULL)
        {
            header.type = CHUNK_FOUND;
            header.size = chunk->size;
        }
        else
        {
            header.type = CHUNK_NOT_FOUND;
            header.size = 0;
        }
        
        if (send_all(client_fd, &header, sizeof(header)) < 0)
        {
            close(client_fd);
            return NULL;
        }
        
        if (chunk != NULL)
        {
            if (send_all(client_fd, chunk->data, chunk->size) < 0)
            {
                close(client_fd);
                return NULL;
            }
        }
    }

    close(client_fd);
    print_client_menu();
    printf("\n");

    return NULL;
}

/**************************************************************************
 *!  static void *start_peer_server(void *arg)
 **************************************************************************
 *  \brief Runs the peer's listening socket, accepting incoming chunk requests from other peers.
 *  \param[in] arg Pointer to the heap-allocated listening port number (freed internally).
 *  \return Always NULL.
 **************************************************************************/
static void *start_peer_server(void *arg)
{
    int port;
    int server_fd;
    int client_fd;
    int *client_fd_ptr;
    int opt;
    struct sockaddr_in server_addr;
    struct sockaddr_in client_addr;
    socklen_t client_len;
    pthread_t peer_thread;
    char client_ip[INET_ADDRSTRLEN];

    port = *(int *)arg;
    free(arg);

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0)
    {
        perror("socket");
        return NULL;
    }

    opt = 1;

    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0)
    {
        perror("setsockopt");
        close(server_fd);
        return NULL;
    }

    memset(&server_addr, 0, sizeof(server_addr));

    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port);

    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
    {
        perror("bind");
        close(server_fd);
        return NULL;
    }

    if (listen(server_fd, 5) < 0)
    {
        perror("listen");
        close(server_fd);
        return NULL;
    }

    printf("Peer server is listening on port %d...\n", port);

    while (1)
    {
        client_len = sizeof(client_addr);

        client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
        if (client_fd < 0)
        {
            perror("accept");
            continue;
        }

        inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, sizeof(client_ip));
        printf("Peer connected: %s\n", client_ip);

        client_fd_ptr = malloc(sizeof(int));
        if (client_fd_ptr == NULL)
        {
            perror("malloc");
            close(client_fd);
            continue;
        }

        *client_fd_ptr = client_fd;

        if (pthread_create(&peer_thread, NULL, handle_peer, client_fd_ptr) != 0)
        {
            perror("pthread_create");
            free(client_fd_ptr);
            close(client_fd);
            continue;
        }

        pthread_detach(peer_thread);
    }
 
    close(server_fd);
 
    return NULL;
}
 
/**************************************************************************
 *!  int main(int argc, char *argv[])
 **************************************************************************
 *  \brief Entry point; starts the peer server thread, registers with the tracker, and runs the interactive download menu.
 *  \param[in] argc Number of command-line arguments.
 *  \param[in] argv Command-line arguments; tracker IP, tracker port, and local peer port.
 *  \return 0 on normal exit, 1 on startup failure.
 **************************************************************************/
int main(int argc, char *argv[])
{
    const char *tracker_ip;
    int tracker_port;
    int peer_port;
    int tracker_fd;
    int *peer_port_ptr;
    struct sockaddr_in tracker_addr;
    char buffer[BUFFER_SIZE];
    char message[BUFFER_SIZE];
    pthread_t peer_thread;
    int file_id;
    int chunk_id;
    int choice;
 
    if (argc != 4)
    {
        printf("Usage: %s <tracker_ip> <tracker_port> <peer_port>\n", argv[0]);
        return 1;
    }
 
    tracker_ip = argv[1];
    tracker_port = atoi(argv[2]);
    peer_port = atoi(argv[3]);

    memset(&local_peer, 0, sizeof(local_peer));
 
    local_peer.address.port = peer_port;
    local_peer.file_count = 0;
 
    peer_port_ptr = malloc(sizeof(int));
    if (peer_port_ptr == NULL)
    {
        perror("malloc");
        return 1;
    }
 
    *peer_port_ptr = peer_port;
 
    if (pthread_create(&peer_thread, NULL, start_peer_server, peer_port_ptr) != 0)
    {
        perror("pthread_create");
        free(peer_port_ptr);
        return 1;
    }
 
    pthread_detach(peer_thread); /* no need to join; runs for the lifetime of the process */

    tracker_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (tracker_fd < 0)
    {
        perror("socket");
        return 1;
    }
 
    memset(&tracker_addr, 0, sizeof(tracker_addr));
 
    tracker_addr.sin_family = AF_INET;
    tracker_addr.sin_port = htons(tracker_port);
 
    if (inet_pton(AF_INET, tracker_ip, &tracker_addr.sin_addr) <= 0)
    {
        perror("inet_pton");
        close(tracker_fd);
        return 1;
    }
 
    if (connect(tracker_fd, (struct sockaddr *)&tracker_addr, sizeof(tracker_addr)) < 0)
    {
        perror("connect");
        close(tracker_fd);
        return 1;
    }
 
    printf("Connected to tracker!\n");
 
    snprintf(message, sizeof(message), "REGISTER %d\n", peer_port);
 
    if (send(tracker_fd, message, strlen(message), 0) < 0)
    {
        perror("send");
        close(tracker_fd);
        return 1;
    }
 
    memset(buffer, 0, sizeof(buffer));
 
    if (recv_until_end(tracker_fd, buffer, sizeof(buffer)) < 0)
    {
        printf("Failed to receive registration response\n");
        close(tracker_fd);
        return 1;
    }
    
    printf("Tracker replied:\n%.*s\n", (int)strcspn(buffer, "\n"), buffer);
    parse_file_list(buffer);

    // update_file_in_tracker(tracker_fd, &local_peer.files[0]);

    while (1)
    {
        print_client_menu();
    
        if (scanf("%d", &choice) != 1)
        {
            printf("Invalid input\n");
            while (getchar() != '\n')
            {
            }
            continue;
        }
        printf("\n\n");
    
        if (choice == 1)
        {
            printf("Enter file ID: ");
            scanf("%d", &file_id);

            download_file(tracker_fd, file_id);
        }
 
        else if (choice == 2)
        {
            printf("Enter file ID: ");
            scanf("%d", &file_id);

            printf("Enter chunk ID: ");
            scanf("%d", &chunk_id);

            download_chunk(tracker_fd, file_id, chunk_id);
        }

        else if (choice == 3)
        {
            if (get_files(tracker_fd) == 0)
            {
                print_available_files();
            }
        }

        else if (choice == 4)
        {
            print_local_files();
        }

        else if (choice == 5)
        {
            send_all(tracker_fd, "DISCONNECT\n", strlen("DISCONNECT\n"));
            close(tracker_fd);
            break;
        }

        else
        {
            printf("Invalid option\n");
        }
    }
 
    return 0;
}
