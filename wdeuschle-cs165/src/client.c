/* This line at the top is necessary for compilation on the lab machine and many other Unix machines.
Please look up _XOPEN_SOURCE for more details. As well, if your code does not compile on the lab
machine please look into this as a a source of error. */
#define _XOPEN_SOURCE

/**
 * client.c
 *  CS165 Fall 2015
 *
 * This file provides a basic unix socket implementation for a client
 * used in an interactive client-server database.
 * The client receives input from stdin and sends it to the server.
 * No pre-processing is done on the client-side.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "common.h"
#include "message.h"
#include "utils.h"

#define DEFAULT_STDIN_BUFFER_SIZE 1024
#define LINE_SIZE 128

void print_int_res(char* payload, int num_entries, int num_columns) {
    int* int_payload = (int*) payload;
    for (int i = 0; i < num_entries; ++i) {
        printf("%d", int_payload[i]);
        if ((i + 1) % num_columns == 0) {
            printf("\n");
        } else {
            printf(",");
        }
    }
}

void print_long_res(char* payload, int num_entries, int num_columns) {
    long* long_payload = (long*) payload;
    for (int i = 0; i < num_entries; ++i) {
        printf("%ld", long_payload[i]);
        if ((i + 1) % num_columns == 0) {
            printf("\n");
        } else {
            printf(",");
        }
    }
}

void print_double_res(char* payload, int num_entries, int num_columns) {
    double* double_payload = (double*) payload;
    for (int i = 0; i < num_entries; ++i) {
        printf("%.2f", double_payload[i]);
        if ((i + 1) % num_columns == 0) {
            printf("\n");
        } else {
            printf(",");
        }
    }
}

/**
 *
 *
 * This method actually sends our queries to the server and listens for its
 * responses.
 *
 **/
void communicate_with_server(int client_socket, message send_message, message recv_message) {
    int len = 0;

    // Send the message_header, which tells server payload size
    if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
        log_err("Failed to send message header.");
        exit(1);
    }

    // Send the payload (query) to server
    if (send(client_socket, send_message.payload, send_message.length, 0) == -1) {
        log_err("Failed to send query payload.");
        exit(1);
    }

    // Always wait for server response (even if it is just an OK message)
    if ((len = recv(client_socket, &(recv_message), sizeof(message), 0)) > 0) {
        if ((recv_message.status == OK_WAIT_FOR_DATA) && (int) recv_message.length > 0) {
            // Calculate number of bytes in response package
            int num_bytes = (int) recv_message.length;
            log_err("num_bytes on client: %d\n", num_bytes);
            int entries;
            char payload[num_bytes];

            // receive each transmission
            int num_batches = number_of_batches(num_bytes);

            for (int i = 0; i < num_batches; ++i) {
                if ((len = recv(client_socket, payload + (i * SOCKET_TRANSMISSION_SIZE), SOCKET_TRANSMISSION_SIZE, 0)) <= 0) {
                    log_err("no bytes transmitted");
                    exit(1);
                }
            }

            if (recv_message.data_type == INT) {
                assert(num_bytes % sizeof(int) == 0);
                entries = num_bytes / sizeof(int);
                print_int_res(payload, entries, recv_message.num_columns);
            } else if (recv_message.data_type == LONG) {
                assert(num_bytes % sizeof(long) == 0);
                entries = num_bytes / sizeof(long);
                print_long_res(payload, entries, recv_message.num_columns);
            } else if (recv_message.data_type == DOUBLE) {
                assert(num_bytes % sizeof(double) == 0);
                entries = num_bytes / sizeof(double);
                print_double_res(payload, entries, recv_message.num_columns);
            } else {
                log_err("No data type specified\n");
                exit(1);
            }
        } else if ((recv_message.status == OK_WAIT_FOR_DATA) && (int) recv_message.length == 0) {
            // unfortunately we have to do this for test 35, an empty join which
            // should have no results
            printf("\n");
        } else if ((recv_message.status == OK_WAIT_FOR_RESPONSE) && (int) recv_message.length > 0) {
            // Calculate number of bytes in response package
            int num_bytes = (int) recv_message.length;
            log_err("num_bytes on client: %d\n", num_bytes);
            char payload[num_bytes + 1];

            // receive each transmission
            int num_batches = number_of_batches(num_bytes);

            for (int i = 0; i < num_batches; ++i) {
                if ((len = recv(client_socket, payload + (i * SOCKET_TRANSMISSION_SIZE), SOCKET_TRANSMISSION_SIZE, 0)) <= 0) {
                    log_err("no bytes transmitted");
                    exit(1);
                }
            }
            // cap transmission and print
            payload[num_bytes] = '\0';
            printf("%s\n", payload);
        } else if ((int) recv_message.length > 0) {
            // Calculate number of bytes in response package
            int num_bytes = (int) recv_message.length;
            char payload[num_bytes + 1];

            // Receive the payload and print it out
            if ((len = recv(client_socket, payload, num_bytes, 0)) > 0) {
                payload[num_bytes] = '\0';
                log_err("%s\n", payload);
            }
        }
    } else {
        if (len < 0) {
            log_err("Failed to receive message.");
        }
        else {
            // don't log this is messes with tests
            log_err("Server closed connection\n");
        }
        exit(1);
    }
}

/**
 * connect_client()
 *
 * This sets up the connection on the client side using unix sockets.
 * Returns a valid client socket fd on success, else -1 on failure.
 *
 **/
int connect_client() {
    int client_socket;
    size_t len;
    struct sockaddr_un remote;

    /*log_info("Attempting to connect...\n");*/

    if ((client_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    remote.sun_family = AF_UNIX;
    strncpy(remote.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    len = strlen(remote.sun_path) + sizeof(remote.sun_family) + 1;
    if (connect(client_socket, (struct sockaddr *)&remote, len) == -1) {
        log_err("client connect failed: ");
        return -1;
    }

     /*log_info("Client connected at socket: %d.\n", client_socket);*/
    return client_socket;
}

int main(void)
{
    int client_socket = connect_client();
    if (client_socket < 0) {
        exit(1);
    }

    message send_message;
    message recv_message;

    // Always output an interactive marker at the start of each command if the
    // input is from stdin. Do not output if piped in from file or from other fd
    char* prefix = "";
    if (isatty(fileno(stdin))) {
        prefix = "db_client > ";
    }

    char *output_str = NULL;

    // Continuously loop and wait for input. At each iteration:
    // 1. output interactive marker
    // 2. read from stdin until eof.
    char read_buffer[DEFAULT_STDIN_BUFFER_SIZE];
    send_message.payload = read_buffer;

    while (printf("%s", prefix), output_str = fgets(read_buffer,
           DEFAULT_STDIN_BUFFER_SIZE, stdin), !feof(stdin)) {
        if (output_str == NULL) {
            log_err("fgets failed.\n");
            break;
        }

        // Only process input that is greater than 1 character.
        // Convert to message and send the message and the
        // payload directly to the server.
        send_message.length = strlen(read_buffer);
        if (send_message.length > 1) {

            if (strncmp(send_message.payload, "load", 4) == 0) {
                // sending data to the server over the wire
                /*log_info("CLIENT LOAD: %s\n", send_message.payload);*/
                // going to be modifying the file name
                char* to_free = malloc(strlen(send_message.payload) + 1);
                char* file_name = to_free;
                strcpy(file_name, send_message.payload);
                file_name = file_name + 4;
                if (strncmp(file_name, "(", 1) == 0) {
                    file_name++;
                    int last_char = strlen(file_name) - 1;
                    // likely has a new line at the end
                    if (file_name[last_char] == '\n') {
                        file_name[last_char] = '\0';
                    }
                    // trim quotes and check for finishing parenthesis.
                    file_name = trim_quotes(file_name);
                    last_char = strlen(file_name) - 1;
                    if (last_char < 0 || file_name[last_char] != ')') {
                        log_err("Incorrect format for load command.\n");
                        free(to_free);
                        exit(1);
                    }
                    // replace final ')' with null-termination character.
                    file_name[last_char] = '\0';

                    FILE* fp = NULL;
                    // open file and validate success
                    fp = fopen(file_name, "r");
                    if (fp == NULL) {
                        log_err("failed to open file");
                        free(to_free);
                        exit(1);
                    }

                    // metadata about the rows we are inserting
                    // e.g. db1.tbl1.col1, db1.tbl1.col2, etc.
                    char metadata[LINE_SIZE];

                    // load in metadata
                    // read first line to get the metadata
                    if (fgets(metadata, LINE_SIZE, fp) == NULL) {
                        // close file
                        fclose(fp);
                        log_err("failed to read metadata from file");
                        free(to_free);
                        exit(1);
                    }

                    // trim metadata to just the table and the column
                    int dot_count = 0;
                    for (unsigned int i = 0; i < strlen(metadata); ++i) {
                        dot_count += metadata[i] == '.';
                        if (dot_count == 2) {
                            metadata[i] = '\0';
                            break;
                        }
                    }

                    /*log_info("TRIMMED METADATA: %s\n", metadata);*/

                    char line[LINE_SIZE];
                    // read rows from the file, and send them as relational inserts
                    char* relational_insert = "relational_insert(";
                    while(fgets(line, LINE_SIZE, fp) != NULL) {
                        // likely has a new line at the end
                        if (line[strlen(line) - 1] == '\n') {
                            line[strlen(line) - 1] = '\0';
                        }
                        // modify send_message to be our query
                        // message to send structure:
                        // "relational_insert(" + "db1.tbl1" + "," + ROW + ")" + "\0"
                        char query_msg[strlen(relational_insert) + strlen(metadata) + strlen(line) + 4];
                        int current_pos = 0;
                        // copy the relational_insert
                        memcpy(&query_msg[current_pos], relational_insert, strlen(relational_insert));
                        // update current_pos
                        current_pos += strlen(relational_insert);
                        // copy the metadata
                        memcpy(&query_msg[current_pos], metadata, strlen(metadata));
                        // update current_pos
                        current_pos += strlen(metadata);
                        // copy the comma
                        query_msg[current_pos] = ',';
                        // update current_pos
                        current_pos += 1;
                        // copy the row data
                        memcpy(&query_msg[current_pos], line, strlen(line));
                        // update current_pos
                        current_pos += strlen(line);
                        // copy the ")"
                        query_msg[current_pos] = ')';
                        // update current_pos
                        current_pos += 1;
                        // copy the "\0"
                        query_msg[current_pos] = '\0';
                        // update current_pos
                        current_pos += 1;

                        // done, copy to read_buffer
                        strcpy(read_buffer, query_msg);
                        send_message.length = strlen(read_buffer);

                        communicate_with_server(client_socket, send_message, recv_message);
                    }
                    // indicate to the server that we've finished a load, so
                    // that it knows to do cleanup in the event we were creating
                    // a table with indexes
                    char* finished_load = "finished_load";
                    strcpy(send_message.payload, finished_load);
                    send_message.length = strlen(finished_load);
                    communicate_with_server(client_socket, send_message, recv_message);

                    free(to_free);
                } else {
                    log_err("Incorrect format for load command.\n");
                    exit(1);
                }
            } else {
                // sending any other db command
                communicate_with_server(client_socket, send_message, recv_message);
            }
        }
    }
    close(client_socket);
    return 0;
}
