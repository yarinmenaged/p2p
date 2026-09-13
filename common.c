#include <arpa/inet.h>
#include <stdio.h>
#include <sys/socket.h>
#include "common.h"

/**************************************************************************
 *!  SharedFile *get_peer_file(Peer *peer, int file_id)
 **************************************************************************
 *  \brief Looks up a file entry already held by a peer by its file ID.
 *  \param[in] peer Peer whose file list is searched.
 *  \param[in] file_id ID of the file to find.
 *  \return Pointer to the matching SharedFile, or NULL if not found.
 **************************************************************************/
SharedFile *get_peer_file(Peer *peer, int file_id)
{
    int i;
 
    for (i = 0; i < peer->file_count; i++)
    {
        if (peer->files[i].file_id == file_id)
        {
            return &peer->files[i];
        }
    }
 
    return NULL;
}

/**************************************************************************
 *!  int recv_all(int socket_fd, void *buffer, int size)
 **************************************************************************
 *  \brief Reads exactly `size` bytes from a socket, retrying until complete.
 *  \param[in] socket_fd Socket descriptor to read from.
 *  \param[in] size Number of bytes to read.
 *  \param[out] buffer Buffer that receives the bytes read.
 *  \return Total bytes received, or -1 on error/disconnect.
 **************************************************************************/
int recv_all(int socket_fd, void *buffer, int size)
{
    int total_received;
    int bytes_received;
 
    total_received = 0;
 
    while (total_received < size)
    {
        bytes_received = recv(socket_fd, (char *)buffer + total_received, size - total_received, 0);
 
        if (bytes_received < 0)
        {
            perror("recv");
            return -1;
        }
 
        if (bytes_received == 0)
        {
            return -1;
        }
 
        total_received += bytes_received;
    }
 
    return total_received;
}

/**************************************************************************
 *!  int send_all(int socket_fd, const void *buffer, int size)
 **************************************************************************
 *  \brief Writes exactly `size` bytes to a socket, retrying until complete.
 *  \param[in] socket_fd Socket descriptor to write to.
 *  \param[in] buffer Data to send.
 *  \param[in] size Number of bytes to send.
 *  \return Total bytes sent, or -1 on error/disconnect.
 **************************************************************************/
int send_all(int socket_fd, const void *buffer, int size)
{
    int total_sent;
    int bytes_sent;
 
    total_sent = 0;
 
    while (total_sent < size)
    {
        bytes_sent = send(socket_fd, (char *)buffer + total_sent, size - total_sent, 0);
 
        if (bytes_sent < 0)
        {
            perror("send");
            return -1;
        }
 
        if (bytes_sent == 0)
        {
            return -1;
        }
 
        total_sent += bytes_sent;
    }
 
    return total_sent;
}

/**************************************************************************
 *!  int recv_until_end(int socket_fd, char *buffer, int size)
 **************************************************************************
 *  \brief Reads from a socket into buffer until the "END\n" marker appears.
 *  \param[in] socket_fd Socket descriptor to read from.
 *  \param[in] size Size of buffer.
 *  \param[out] buffer Buffer that accumulates the null-terminated data received.
 *  \return Total bytes received, or -1 on error or if END was never received.
 **************************************************************************/
int recv_until_end(int socket_fd, char *buffer, int size)
{
    int total_received;
    int bytes_received;
 
    total_received = 0;
 
    while (total_received < size - 1)
    {
        bytes_received = recv(socket_fd,
                               buffer + total_received,
                               size - 1 - total_received,
                               0);
 
        if (bytes_received < 0)
        {
            perror("recv");
            return -1;
        }
 
        if (bytes_received == 0)
        {
            return -1;
        }
 
        total_received += bytes_received;
        buffer[total_received] = '\0';
 
        if (strstr(buffer, "END\n") != NULL)
        {
            return total_received;
        }
    }
 
    printf("Buffer is full before END was received\n");
    return -1;
}