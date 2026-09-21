#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define TRACKER_PORT 6000
#define CLIENT1_PORT 6001
#define CLIENT2_PORT 6002
#define CLIENT3_PORT 6003

#define EXAMPLE_FILE_NAME "example_file.txt"
#define FILE1_NAME "file1"
#define FILE2_NAME "file2"

#define EXAMPLE_FILE_ID 1
#define FILE1_ID 2
#define FILE2_ID 3
#define FILE1_TOTAL_CHUNKS 5

#define TRACKER_LOG "tests/logs/tracker.log"
#define CLIENT1_LOG "tests/logs/client1.log"
#define CLIENT2_LOG "tests/logs/client2.log"
#define CLIENT3_LOG "tests/logs/client3.log"
#define TEST_LOG "tests/logs/test_summery.log"

#define TEST_LOG_DIR "tests/logs"

#define COMMAND_DELAY_US 1000000
#define STARTUP_DELAY_US 2000000
#define DOWNLOAD_DELAY_US 4000000

typedef struct
{
    pid_t pid;
    int stdin_fd;
    const char *log_file;
} TestProcess;

static TestProcess tracker;
static TestProcess client1;
static TestProcess client2;
static TestProcess client3;

static int tests_passed;
static int tests_failed;

static void sleep_us(useconds_t microseconds)
{
    usleep(microseconds);
}

static void print_result(const char *test_name, int passed)
{
    FILE *log_file;
    const char *status;

    status = passed ? "[PASS]" : "[FAIL]";

    printf("%s %s\n", status, test_name);

    log_file = fopen(TEST_LOG, "a");
    if (log_file != NULL)
    {
        fprintf(log_file, "%s %s\n", status, test_name);
        fclose(log_file);
    }

    if (passed)
    {
        tests_passed++;
    }
    else
    {
        tests_failed++;
    }

    fflush(stdout);
}

static int clean_server_files(void)
{
    DIR *directory;
    struct dirent *entry;
    char path[512];

    directory = opendir("server/server_files");

    if (directory == NULL)
    {
        perror("opendir server/server_files");
        return -1;
    }

    while ((entry = readdir(directory)) != NULL)
    {
        if (strcmp(entry->d_name, ".") == 0 ||
            strcmp(entry->d_name, "..") == 0 ||
            strcmp(entry->d_name, EXAMPLE_FILE_NAME) == 0)
        {
            continue;
        }

        snprintf(path, sizeof(path), "server/server_files/%s", entry->d_name);

        if (unlink(path) < 0)
        {
            perror("unlink");
            closedir(directory);
            return -1;
        }
    }

    closedir(directory);

    return 0;
}

static int create_directory(const char *path)
{
    if (mkdir(path, 0755) < 0 && errno != EEXIST)
    {
        return -1;
    }

    return 0;
}

static int create_pipe(int pipe_fd[2])
{
    if (pipe(pipe_fd) < 0)
    {
        perror("pipe");
        return -1;
    }

    return 0;
}

static pid_t start_program(const char *program, char *const argv[], const char *log_file, int *stdin_fd)
{
    int stdin_pipe[2];
    int log_fd;
    pid_t pid;

    if (create_pipe(stdin_pipe) < 0)
    {
        return -1;
    }

    pid = fork();

    if (pid < 0)
    {
        perror("fork");
        close(stdin_pipe[0]);
        close(stdin_pipe[1]);
        return -1;
    }

    if (pid == 0)
    {
        log_fd = open(log_file, O_WRONLY | O_CREAT | O_TRUNC, 0644);

        if (log_fd < 0)
        {
            perror("open log");
            _exit(1);
        }

        if (dup2(stdin_pipe[0], STDIN_FILENO) < 0)
        {
            perror("dup2 stdin");
            _exit(1);
        }

        if (dup2(log_fd, STDOUT_FILENO) < 0)
        {
            perror("dup2 stdout");
            _exit(1);
        }

        if (dup2(log_fd, STDERR_FILENO) < 0)
        {
            perror("dup2 stderr");
            _exit(1);
        }

        close(stdin_pipe[0]);
        close(stdin_pipe[1]);
        close(log_fd);

        execv(program, argv);

        perror("execv");
        _exit(1);
    }

    close(stdin_pipe[0]);
    *stdin_fd = stdin_pipe[1];

    return pid;
}

static int send_command(int stdin_fd, const char *command)
{
    size_t length;
    ssize_t bytes_written;

    length = strlen(command);

    bytes_written = write(stdin_fd, command, length);

    if (bytes_written != (ssize_t)length)
    {
        return -1;
    }

    /*
     * The client/tracker print their menu with fflush(stdout).
     * Waiting here gives the child enough time to finish the command
     * and flush its output before the test reads the log.
     */
    sleep_us(COMMAND_DELAY_US);

    return 0;
}

static long get_log_offset(const char *filename)
{
    struct stat file_stat;

    if (stat(filename, &file_stat) < 0)
    {
        return 0;
    }

    return file_stat.st_size;
}

static int log_contains_from(const char *filename, long offset, const char *text)
{
    FILE *file;
    char line[2048];

    file = fopen(filename, "r");

    if (file == NULL)
    {
        return 0;
    }

    if (fseek(file, offset, SEEK_SET) != 0)
    {
        fclose(file);
        return 0;
    }

    while (fgets(line, sizeof(line), file) != NULL)
    {
        if (strstr(line, text) != NULL)
        {
            fclose(file);
            return 1;
        }
    }

    fclose(file);

    return 0;
}

static int download_completed_successfully(const char *log_file, long offset)
{
    return log_contains_from(log_file, offset, "All chunks of file 2 downloaded successfully");
}

static int server_files_are_correct(const char *log_file, long offset)
{
    if (!log_contains_from(log_file, offset, EXAMPLE_FILE_NAME))
    {
        return 0;
    }

    if (!log_contains_from(log_file, offset, FILE1_NAME))
    {
        return 0;
    }

    if (log_contains_from(log_file, offset, FILE2_NAME))
    {
        return 0;
    }

    return 1;
}

static int client_has_file(const char *log_file, long offset, const char *filename)
{
    return log_contains_from(log_file, offset, filename);
}

static int client_has_chunk(const char *log_file, long offset, int chunk_id)
{
    char expected[64];

    snprintf(expected, sizeof(expected), "[%d]", chunk_id);

    if (log_contains_from(log_file, offset, expected))
    {
        return 1;
    }

    snprintf(expected, sizeof(expected), "chunk %d", chunk_id);

    return log_contains_from(log_file, offset, expected);
}

static int client_has_complete_file(const char *log_file, long offset, const char *filename, int total_chunks)
{
    char progress[64];

    if (!log_contains_from(log_file, offset, filename))
    {
        return 0;
    }

    snprintf(progress, sizeof(progress), "%d/%d", total_chunks, total_chunks);

    return log_contains_from(log_file, offset, progress);
}

static int file_compare(const char *first_filename, const char *second_filename)
{
    int first_fd;
    int second_fd;
    char first_buffer[4096];
    char second_buffer[4096];
    ssize_t first_bytes;
    ssize_t second_bytes;

    first_fd = open(first_filename, O_RDONLY);

    if (first_fd < 0)
    {
        return 0;
    }

    second_fd = open(second_filename, O_RDONLY);

    if (second_fd < 0)
    {
        close(first_fd);
        return 0;
    }

    while (1)
    {
        first_bytes = read(first_fd, first_buffer, sizeof(first_buffer));
        second_bytes = read(second_fd, second_buffer, sizeof(second_buffer));

        if (first_bytes != second_bytes)
        {
            close(first_fd);
            close(second_fd);
            return 0;
        }

        if (first_bytes == 0)
        {
            break;
        }

        if (memcmp(first_buffer, second_buffer, first_bytes) != 0)
        {
            close(first_fd);
            close(second_fd);
            return 0;
        }
    }

    close(first_fd);
    close(second_fd);

    return 1;
}

static int process_is_running(pid_t pid)
{
    if (pid <= 0)
    {
        return 0;
    }

    if (kill(pid, 0) == 0)
    {
        return 1;
    }

    return errno == EPERM;
}

static int wait_for_process_exit(TestProcess *process)
{
    int status;
    pid_t result;

    if (process->pid <= 0)
    {
        return 1;
    }

    result = waitpid(process->pid, &status, WNOHANG);

    if (result == 0)
    {
        return 0;
    }

    if (result == process->pid)
    {
        close(process->stdin_fd);
        process->stdin_fd = -1;
        process->pid = -1;
        return 1;
    }

    return 0;
}

static void stop_process(TestProcess *process)
{
    int status;

    if (process->pid <= 0)
    {
        return;
    }

    if (process_is_running(process->pid))
    {
        kill(process->pid, SIGTERM);
        sleep_us(300000);

        if (process_is_running(process->pid))
        {
            kill(process->pid, SIGKILL);
        }
    }

    waitpid(process->pid, &status, 0);

    if (process->stdin_fd >= 0)
    {
        close(process->stdin_fd);
    }

    process->pid = -1;
    process->stdin_fd = -1;
}

static void cleanup(void)
{
    stop_process(&client3);
    stop_process(&client2);
    stop_process(&client1);
    stop_process(&tracker);
}

static void initialize_processes(void)
{
    memset(&tracker, 0, sizeof(tracker));
    memset(&client1, 0, sizeof(client1));
    memset(&client2, 0, sizeof(client2));
    memset(&client3, 0, sizeof(client3));

    tracker.pid = -1;
    client1.pid = -1;
    client2.pid = -1;
    client3.pid = -1;

    tracker.stdin_fd = -1;
    client1.stdin_fd = -1;
    client2.stdin_fd = -1;
    client3.stdin_fd = -1;
}

int main(void)
{
    char *tracker_argv[] = {"./bin/tracker", "6000", NULL};
    char *client1_argv[] = {"./bin/client", "127.0.0.1", "6000", "6001", NULL};
    char *client2_argv[] = {"./bin/client", "127.0.0.1", "6000", "6002", NULL};
    char *client3_argv[] = {"./bin/client", "127.0.0.1", "6000", "6003", NULL};

    char client3_file[256];
    long log_offset;
    int tracker_started;
    int client1_started;
    int client2_started;
    int client3_started;

    initialize_processes();

    if (clean_server_files() < 0)
    {
        printf("[FAIL] Could not clean server/server_files");
        return 1;
    }

    if (create_directory("tests") < 0 ||
        create_directory(TEST_LOG_DIR) < 0)
    {
        printf("[FAIL] Could not create tests/logs directory\n");
        return 1;
    }

    unlink(TRACKER_LOG);
    unlink(CLIENT1_LOG);
    unlink(CLIENT2_LOG);
    unlink(CLIENT3_LOG);
    unlink(TEST_LOG);

    printf("========================================\n");
    printf("          P2P GLOBAL SYSTEM TEST\n");
    printf("========================================\n\n");

    /*
     * 1. Start Tracker.
     */
    tracker.pid = start_program("./bin/tracker", tracker_argv, TRACKER_LOG, &tracker.stdin_fd);
    tracker_started = tracker.pid > 0;

    if (tracker_started)
    {
        sleep_us(STARTUP_DELAY_US);
    }

    print_result("1. Tracker started",
                 tracker_started && process_is_running(tracker.pid));

    if (!tracker_started)
    {
        cleanup();
        return 1;
    }

    /*
     * 2. Add file1 with 5000 bytes.
     */
    log_offset = get_log_offset(TRACKER_LOG);
    send_command(tracker.stdin_fd, "1\nfile1\n5000\n");

    print_result("2. Added file1 (5000 bytes)",
                 log_contains_from(TRACKER_LOG, log_offset, FILE1_NAME));

    /*
     * 3. Add file2 with 3000 bytes.
     */
    log_offset = get_log_offset(TRACKER_LOG);
    send_command(tracker.stdin_fd, "1\nfile2\n3000\n");

    print_result("3. Added file2 (3000 bytes)",
                 log_contains_from(TRACKER_LOG, log_offset, FILE2_NAME));

    /*
     * 4. Delete file2.
     */
    send_command(tracker.stdin_fd, "2\n3\n");

    log_offset = get_log_offset(TRACKER_LOG);
    send_command(tracker.stdin_fd, "3\n");

    print_result("4. Deleted file2",
                 !log_contains_from(TRACKER_LOG, log_offset, FILE2_NAME));

    /*
     * 5. Tracker tries to delete a file that does not exist.
     */
    log_offset = get_log_offset(TRACKER_LOG);
    send_command(tracker.stdin_fd, "2\n999\n");

    print_result("5. Tracker rejects deletion of non-existent file",
                 log_contains_from(TRACKER_LOG, log_offset, "File 999 not found"));

    /*
     * 6. Show server files.
     *
     * Only output generated by this Show files command is examined.
     * Old occurrences of file2 in the log are therefore irrelevant.
     */
    log_offset = get_log_offset(TRACKER_LOG);
    send_command(tracker.stdin_fd, "3\n");

    print_result("6. Server contains example_file and file1 only",
                 server_files_are_correct(TRACKER_LOG, log_offset));

    /*
     * 7. Start Client 1.
     */
    client1.pid = start_program("./bin/client", client1_argv, CLIENT1_LOG, &client1.stdin_fd);
    client1_started = client1.pid > 0;

    if (client1_started)
    {
        sleep_us(STARTUP_DELAY_US);
    }

    print_result("7. Client 1 started",
                 client1_started && process_is_running(client1.pid));

    /*
     * 8. Client 1 shows server files.
     */
    log_offset = get_log_offset(CLIENT1_LOG);
    send_command(client1.stdin_fd, "3\n");

    print_result("8. Client 1 received server file list",
                 client_has_file(CLIENT1_LOG, log_offset, EXAMPLE_FILE_NAME) &&
                 client_has_file(CLIENT1_LOG, log_offset, FILE1_NAME));

    /*
     * 9. Client 1 tries to download a file that does not exist.
     */
    log_offset = get_log_offset(CLIENT1_LOG);
    send_command(client1.stdin_fd, "1\n999\n");

    print_result("9. Client 1 rejects non-existent file",
                 log_contains_from(CLIENT1_LOG, log_offset, "File 999 not found"));

    /*
     * 10. Client 1 tries to download a chunk from a file that does not exist.
     */
    log_offset = get_log_offset(CLIENT1_LOG);
    send_command(client1.stdin_fd, "2\n999\n0\n");

    print_result("10. Client 1 rejects chunk from non-existent file",
                 log_contains_from(CLIENT1_LOG, log_offset, "File 999 doesn't exist."));

    /*
     * 11. Client 1 tries to download a non-existent chunk from file1.
     */
    log_offset = get_log_offset(CLIENT1_LOG);
    send_command(client1.stdin_fd, "2\n2\n5\n");

    print_result("11. Client 1 rejects non-existent chunk",
                 log_contains_from(CLIENT1_LOG, log_offset, "Chunk 5 doesn't exist in file 2."));

    /*
     * 12. Client 1 downloads example_file.
     */
    log_offset = get_log_offset(CLIENT1_LOG);
    send_command(client1.stdin_fd, "1\n1\n");
    sleep_us(DOWNLOAD_DELAY_US);

    print_result("12. Client 1 downloaded example_file",
                 client_has_file(CLIENT1_LOG, log_offset, EXAMPLE_FILE_NAME));

    /*
     * 13. Client 1 tries to download example_file again.
     */
    log_offset = get_log_offset(CLIENT1_LOG);
    send_command(client1.stdin_fd, "1\n1\n");

    print_result("13. Client 1 rejects duplicate file download",
                 log_contains_from(CLIENT1_LOG, log_offset,
                                    "File 1 already fully downloaded on this client"));

    /*
     * 14. Client 1 shows local files.
     */
    log_offset = get_log_offset(CLIENT1_LOG);
    send_command(client1.stdin_fd, "4\n");

    print_result("14. Client 1 contains example_file",
                 client_has_file(CLIENT1_LOG, log_offset, EXAMPLE_FILE_NAME));

    /*
     * 15. Client 1 downloads file1 chunk 0.
     */
    log_offset = get_log_offset(CLIENT1_LOG);
    send_command(client1.stdin_fd, "2\n2\n0\n");
    sleep_us(DOWNLOAD_DELAY_US);

    print_result("15. Client 1 downloaded file1 chunk 0",
                 client_has_chunk(CLIENT1_LOG, log_offset, 0));

    /*
     * 16. Client 1 tries to download file1 chunk 0 again.
     */
    log_offset = get_log_offset(CLIENT1_LOG);
    send_command(client1.stdin_fd, "2\n2\n0\n");

    print_result("16. Client 1 rejects duplicate chunk download",
                 log_contains_from(CLIENT1_LOG, log_offset,
                                    "Chunk 0 of file 2 already exists on the current peer."));

    /*
     * 17. Client 1 shows local files.
     */
    log_offset = get_log_offset(CLIENT1_LOG);
    send_command(client1.stdin_fd, "4\n");

    print_result("17. Client 1 contains file1 chunk 0",
                 client_has_chunk(CLIENT1_LOG, log_offset, 0));

    /*
     * 18. Start Client 2 and Client 3, download the required chunks,
     * and verify the complete file on Client 3.
     */
    client2.pid = start_program("./bin/client", client2_argv, CLIENT2_LOG, &client2.stdin_fd);
    client2_started = client2.pid > 0;

    if (client2_started)
    {
        sleep_us(STARTUP_DELAY_US);
    }

    log_offset = get_log_offset(CLIENT2_LOG);
    send_command(client2.stdin_fd, "2\n2\n1\n");
    sleep_us(DOWNLOAD_DELAY_US);

    {
        long client2_download_offset;
        int client2_download_ok;
        int client2_local_ok;

        client2_download_offset = log_offset;
        client2_download_ok = client_has_chunk(CLIENT2_LOG, client2_download_offset, 1);

        log_offset = get_log_offset(CLIENT2_LOG);
        send_command(client2.stdin_fd, "4\n");
        client2_local_ok = client_has_chunk(CLIENT2_LOG, log_offset, 1);

        client2_download_ok = client2_download_ok && client2_local_ok;

        client3.pid = start_program("./bin/client", client3_argv, CLIENT3_LOG, &client3.stdin_fd);
        client3_started = client3.pid > 0;

        if (client3_started)
        {
            sleep_us(STARTUP_DELAY_US);
        }

        log_offset = get_log_offset(TRACKER_LOG);
        send_command(tracker.stdin_fd, "4\n");

        client2_download_ok = client2_download_ok &&
                              client3_started &&
                              process_is_running(client3.pid) &&
                              log_contains_from(TRACKER_LOG, log_offset, "6001") &&
                              log_contains_from(TRACKER_LOG, log_offset, "6002") &&
                              log_contains_from(TRACKER_LOG, log_offset, "6003");

        log_offset = get_log_offset(CLIENT3_LOG);
        send_command(client3.stdin_fd, "1\n2\n");
        sleep_us(DOWNLOAD_DELAY_US);

        {
            long client3_download_offset;
            int client3_download_ok;
            int client3_complete_ok;

            client3_download_offset = log_offset;
            client3_download_ok = download_completed_successfully(CLIENT3_LOG,
                                                                    client3_download_offset);

            log_offset = get_log_offset(CLIENT3_LOG);
            send_command(client3.stdin_fd, "4\n");
            client3_complete_ok = client_has_complete_file(CLIENT3_LOG, log_offset,
                                                            FILE1_NAME, FILE1_TOTAL_CHUNKS);

            print_result("18. Clients 2 and 3 completed the required downloads",
                         client2_started &&
                         process_is_running(client2.pid) &&
                         client2_download_ok &&
                         client3_download_ok &&
                         client3_complete_ok);
        }
    }

    /*
     * 19. Client 1 owns file1 chunk 0.
     */
    log_offset = get_log_offset(CLIENT1_LOG);
    send_command(client1.stdin_fd, "4\n");

    print_result("19. Client 1 owns file1 chunk 0",
                 client_has_chunk(CLIENT1_LOG, log_offset, 0));

    /*
     * 20. Client 2 owns file1 chunk 1.
     */
    log_offset = get_log_offset(CLIENT2_LOG);
    send_command(client2.stdin_fd, "4\n");

    print_result("20. Client 2 owns file1 chunk 1",
                 client_has_chunk(CLIENT2_LOG, log_offset, 1));

    /*
     * 21. file1 exists in Client 3 directory.
     */
    snprintf(client3_file, sizeof(client3_file),
             "client/client_files/%d/%s", CLIENT3_PORT, FILE1_NAME);

    if (access(client3_file, F_OK) != 0)
    {
        snprintf(client3_file, sizeof(client3_file),
                 "client_files/%d/%s", CLIENT3_PORT, FILE1_NAME);
    }

    print_result("21. file1 exists in Client 3 directory",
                 access(client3_file, F_OK) == 0);

    /*
     * 22. Client 3 file1 matches the server file.
     */
    print_result("22. file1 matches server file",
                 file_compare("server/server_files/file1", client3_file));

    /*
     * 23. Disconnect all Clients.
     */
    send_command(client1.stdin_fd, "5\n");
    send_command(client2.stdin_fd, "5\n");
    send_command(client3.stdin_fd, "5\n");

    sleep_us(2000000);

    /*
     * Reap exited children before checking their state.
     */
    wait_for_process_exit(&client1);
    wait_for_process_exit(&client2);
    wait_for_process_exit(&client3);

    print_result("23. All three Clients disconnected",
                 client1.pid == -1 &&
                 client2.pid == -1 &&
                 client3.pid == -1);

    /*
     * 24. Tracker prints active peers.
     */
    log_offset = get_log_offset(TRACKER_LOG);
    send_command(tracker.stdin_fd, "4\n");

    print_result("24. Tracker has no active peers",
                 !log_contains_from(TRACKER_LOG, log_offset, "6001") &&
                 !log_contains_from(TRACKER_LOG, log_offset, "6002") &&
                 !log_contains_from(TRACKER_LOG, log_offset, "6003"));

    cleanup();

    printf("\n========================================\n");
    printf("              TEST SUMMARY\n");
    printf("========================================\n");
    printf("Passed: %d\n", tests_passed);
    printf("Failed: %d\n", tests_failed);
    printf("========================================\n");

    {
        FILE *log_file = fopen(TEST_LOG, "a");

        if (log_file != NULL)
        {
            fprintf(log_file, "\n========================================\n");
            fprintf(log_file, "              TEST SUMMARY\n");
            fprintf(log_file, "========================================\n");
            fprintf(log_file, "Passed: %d\n", tests_passed);
            fprintf(log_file, "Failed: %d\n", tests_failed);
            fprintf(log_file, "========================================\n");
            fclose(log_file);
        }
    }

    return tests_failed == 0 ? 0 : 1;
}
