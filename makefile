CC = gcc
CFLAGS = -Wall -Wextra
PTHREAD = -pthread
BIN = bin
 
all: $(BIN)/tracker $(BIN)/client tests/test

$(BIN):
	mkdir -p $(BIN)

$(BIN)/tracker: server/tracker.c server/tracker_files.c server/tracker_peers.c server/tracker_files.h server/tracker_peers.h common.c | $(BIN)
	$(CC) $(CFLAGS) server/tracker.c server/tracker_files.c server/tracker_peers.c common.c -o $(BIN)/tracker $(PTHREAD)

$(BIN)/client: client/client.c client/client_files.c client/client_transfer.c client/client_files.h client/client_transfer.h common.c common.h | $(BIN)
	$(CC) $(CFLAGS) client/client.c client/client_files.c client/client_transfer.c common.c -o $(BIN)/client $(PTHREAD)

test: tests/test.c
	$(CC) $(CFLAGS) tests/test.c -o tests/test

clean:
	rm -rf $(BIN)
	rm -rf tests/test 
 