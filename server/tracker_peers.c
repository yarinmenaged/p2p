#include <stdio.h>
#include <string.h>
#include "tracker_peers.h"

Peer peers[MAX_PEERS];
int peer_count = 0;
int next_peer_index = 0;
pthread_mutex_t peers_mutex = PTHREAD_MUTEX_INITIALIZER;

/**************************************************************************
 *!  void remove_peer(int client_fd)
 **************************************************************************
 *  \brief Removes a disconnected peer from the active peer registry and fixes up indices.
 *  \param[in] client_fd Socket descriptor of the peer to remove.
 *  \return None
 **************************************************************************/
void remove_peer(int client_fd)
{
    int i;
    int j;

    for (i = 0; i < peer_count; i++)
    {
        if (peers[i].socket_fd != client_fd)
        {
            continue;
        }

        for (j = i; j < peer_count - 1; j++)
        {
            peers[j] = peers[j + 1];
        }

        peer_count--;

        if (peer_count == 0)
        {
            next_peer_index = 0;
        }
        else if (next_peer_index > i)
        {
            next_peer_index--;
        }
        else if (next_peer_index >= peer_count)
        {
            next_peer_index = 0;
        }

        printf("Peer disconnected: %s:%d\n", peers[i].address.ip, peers[i].address.port);

        return;
    }
}

/**************************************************************************
 *!  void print_active_peers(void)
 **************************************************************************
 *  \brief Prints a formatted table of all currently registered peers.
 *  \return None
 **************************************************************************/
void print_active_peers(void)
{
    int i;

    pthread_mutex_lock(&peers_mutex);

    printf("\n===== Active Peers =====\n\n");
    printf("%-5s %-20s %-10s\n", "ID", "IP Address", "Port");

    for (i = 0; i < peer_count; i++)
    {
        printf("%-5d %-20s %-10d\n",
               i + 1,
               peers[i].address.ip,
               peers[i].address.port);
    }

    printf("\n");

    pthread_mutex_unlock(&peers_mutex);
}

/**************************************************************************
 *!  void select_chunk_peer(int file_id, int chunk_id, const char *client_ip, int peer_port, ChunkSourceHeader *source_header)
 **************************************************************************
 *  \brief Picks a peer (round-robin, excluding the requester) holding the requested chunk, falling back to the server.
 *  \param[in] file_id ID of the file being requested.
 *  \param[in] chunk_id ID of the chunk being requested.
 *  \param[in] client_ip IP address of the requesting peer, used to avoid self-selection.
 *  \param[in] peer_port Port of the requesting peer, used to avoid self-selection.
 *  \param[out] source_header Filled with the chosen source (peer or server).
 *  \return None
 **************************************************************************/
void select_chunk_peer(int file_id, int chunk_id, const char *client_ip, int peer_port, ChunkSourceHeader *source_header)
{
    int i;
    int j;
    int index;
    SharedFile *file;

    pthread_mutex_lock(&peers_mutex);

    for (i = 0; i < peer_count; i++)
    {
        if (peer_count == 0)
        {
            source_header->type = SOURCE_SERVER;
            pthread_mutex_unlock(&peers_mutex);
            return;
        }

        index = (next_peer_index + i) % peer_count;

        if (strcmp(peers[index].address.ip, client_ip) == 0 &&
            peers[index].address.port == peer_port)
        {
            continue;
        }

        file = get_peer_file(&peers[index], file_id);

        if (file == NULL)
        {
            continue;
        }

        for (j = 0; j < file->chunk_count; j++)
        {
            if (file->chunks[j].id == chunk_id)
            {
                source_header->type = SOURCE_PEER;
                source_header->address = peers[index].address;

                next_peer_index = (index + 1) % peer_count;

                pthread_mutex_unlock(&peers_mutex);
                return;
            }
        }
    }

    source_header->type = SOURCE_SERVER;
    pthread_mutex_unlock(&peers_mutex);

    return;
}
