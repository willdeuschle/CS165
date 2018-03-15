/** server.c
 * CS165 Fall 2015
 *
 * This file provides a basic unix socket implementation for a server
 * used in an interactive client-server database.
 * The client should be able to send messages containing queries to the
 * server.  When the server receives a message, it must:
 * 1. Respond with a status based on the query (OK, UNKNOWN_QUERY, etc.)
 * 2. Process any appropriate queries, if applicable.
 * 3. Return the query response to the client.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>
#include <time.h>

#include "common.h"
#include "parse.h"
#include "cs165_api.h"
#include "db_manager.h"
#include "db_updates.h"
#include "db_reads.h"
#include "message.h"
#include "utils.h"
#include "client_context.h"
#include "batch_manager.h"
#include "db_join.h"

#define DEFAULT_QUERY_BUFFER_SIZE 1024
#define CLIENT_CONTEXT_SIZE_START 16

// set server alive
bool keep_server_alive = true;
// we aren't currently loading a table with an index
bool btree_indexed_load = false;
// no table being indexed as of now
Table* btree_indexed_table = NULL;
// we aren't batching queries by default
bool currently_batching_query = false;
SharedScanDbOperators shared_scan_operators;


/*
 * This function cleans up a client context before we exit.
 */
void client_context_free(ClientContext* client_context) {
    for (int i = 0; i < client_context->chandles_in_use; ++i) {
        GeneralizedColumnType type = client_context->chandle_table[i].generalized_column.column_type;
        if (type == RESULT) {
            free(client_context->chandle_table[i].generalized_column.column_pointer.result->payload);
            free(client_context->chandle_table[i].generalized_column.column_pointer.result);
        } else {
            free(client_context->chandle_table[i].generalized_column.column_pointer.column->data);
            free(client_context->chandle_table[i].generalized_column.column_pointer.column);
        }
    }
    free(client_context->chandle_table);
    free(client_context);
}

/** execute_DbOperator takes as input the DbOperator and executes the query.
 * This should be replaced in your implementation (and its implementation possibly moved to a different file).
 * It is currently here so that you can verify that your server and client can send messages.
 **/
void execute_DbOperator(DbOperator* query, message* send_message) {
    if (query == NULL) {
        return;
    }
    if (query->type == CREATE_DB) {
        create_db(query, send_message);
    } else if (query->type == CREATE_TABLE) {
        create_table(query, send_message);
    } else if (query->type == CREATE_COLUMN) {
        create_column(query, send_message);
    } else if (query->type == CREATE_INDEX) {
        create_index(query, send_message);
    } else if (query->type == SHUTDOWN) {
        const char* result_message = "shutdown successful";
        char* result_message_ptr = malloc(strlen(result_message) + 1);
        strcpy(result_message_ptr, result_message);
        send_message->payload = result_message_ptr;
        send_message->status = OK_DONE;
    } else if (query->type == SHARED_QUERY_LOGGED) {
        const char* result_message = "shared scan logged successfully";
        char* result_message_ptr = malloc(strlen(result_message) + 1);
        strcpy(result_message_ptr, result_message);
        send_message->payload = result_message_ptr;
        send_message->status = OK_DONE;
    } else if (query->type == PRINT) {
        // timing
        // ******************
        clock_t start, end;
        double cpu_time_used;
        start = clock();
        // ******************

        // serialize results to send to client
        db_print(query, send_message);

        // timing
        // ******************
        end = clock();
        cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
        log_timing("time to print: %f\n", cpu_time_used);
    // ******************

    } else if (query->type == INSERT) {
        // insert the row
        db_insert(query, send_message);
    } else if (query->type == UPDATE) {
        // update
        db_update(query, send_message);
    } else if (query->type == DELETE) {
        // delete
        db_delete(query, send_message);
    } else if (query->type == SELECT) {
        // select the data
        db_select(query, send_message);
    } else if (query->type == FETCH) {
        // fetch the data
        // timing
        // ******************
        clock_t start, end;
        double cpu_time_used;
        start = clock();
        // ******************

        db_fetch(query, send_message);

        // timing
        // ******************
        end = clock();
        cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
        log_timing("time to fetch: %f\n", cpu_time_used);
        // ******************
    } else if (query->type == JOIN) {
        db_join(query, send_message);
    } else if (query->type == AVERAGE) {
        // average the data
        db_average(query, send_message);
    } else if (query->type == SUM) {
        // sum the data
        db_sum(query, send_message);
    } else if (query->type == MIN) {
        // find the min
        db_min(query, send_message);
    } else if (query->type == MAX) {
        // find the max
        db_max(query, send_message);
    } else if (query->type == ADD) {
        // add the columns, multiplying the second column by 1
        db_add(query, send_message, 1);
    } else if (query->type == SUB) {
        // add the columns, multiplying the second column by -1
        db_add(query, send_message, -1);
    } else if (query->type == SHARED_SCAN) {
        // execute shared scan

        // timing
        // ******************
        clock_t start, end;
        double cpu_time_used;

        start = clock();
        execute_shared_scan(query, send_message);
        end = clock();
        cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
        log_timing("time to execute batch scan: %f\n", cpu_time_used);

        // timing
        // ******************
    }
    return;
}

/**
 *
 *
 * This method sends data to the client.
 *
 **/
void communicate_with_client(int client_socket, message send_message){
    if (send_message.status != OK_WAIT_FOR_DATA) {
        // in this case, we've already serialized the data
        send_message.length = strlen(send_message.payload);
    }

    // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
    if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
        log_err("Failed to send message.");
        exit(1);
    }

    // send in batches
    int num_batches = number_of_batches(send_message.length);

    for (int i = 0; i < num_batches; ++i) {
        size_t num_bytes = SOCKET_TRANSMISSION_SIZE;
        // might be smaller if this is the last batch of bytes
        if (send_message.length - (SOCKET_TRANSMISSION_SIZE * i) < SOCKET_TRANSMISSION_SIZE) {
            num_bytes = send_message.length - (SOCKET_TRANSMISSION_SIZE * i);
        }
        // 4. Send response of request
        if (send(client_socket, send_message.payload + (i * SOCKET_TRANSMISSION_SIZE), num_bytes, 0) == -1) {
            log_err("Failed to send message.");
            exit(1);
        }
    }
    
    return;
}

/**
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/
void handle_client(int client_socket) {
    int done = 0;
    int length = 0;

    log_info("Connected to socket: %d.\n", client_socket);

    // Create two messages, one from which to read and one from which to receive
    message send_message;
    message recv_message;

    // create the client context here
    ClientContext* client_context = malloc(sizeof(ClientContext));
    // column handles
    client_context->chandle_table = malloc(CLIENT_CONTEXT_SIZE_START * sizeof(GeneralizedColumnHandle));
    client_context->chandles_in_use = 0;
    client_context->chandle_slots = CLIENT_CONTEXT_SIZE_START;

    // set up the shared scan operators here
    shared_scan_operators.num_dbos = 0;
    shared_scan_operators.dbo_slots = 0; // SHARED_QUERY_START_SIZE;
    shared_scan_operators.dbos = NULL; // malloc(sizeof(DbOperator*) * SHARED_QUERY_START_SIZE);
    shared_scan_operators.col = NULL;
    shared_scan_operators.num_entries = 0;


    // Continually receive messages from client and execute queries.
    // 1. Parse the command
    // 2. Handle request if appropriate
    // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
    // 4. Send response of request.
    do {
        length = recv(client_socket, &recv_message, sizeof(message), 0);
        if (length < 0) {
            log_err("Client connection closed!\n");
            exit(1);
        } else if (length == 0) {
            done = 1;
        }

        if (!done) {
            char recv_buffer[recv_message.length + 1];
            length = recv(client_socket, recv_buffer, recv_message.length,0);
            recv_message.payload = recv_buffer;
            recv_message.payload[recv_message.length] = '\0';
            send_message.payload = NULL;

            // 1. Parse command
            DbOperator* query = parse_command(recv_message.payload, &send_message, client_socket, client_context);

            // 2. Handle request
            execute_DbOperator(query, &send_message);
            // free the query
            db_operator_free(query);

            // if there is no message, indicate that
            // note that we pass over OK_WAIT_FOR_DATA empty payloads, this is
            // because the tests require us to print a newline for these empty
            // payloads
            if (send_message.payload == NULL && send_message.status != OK_WAIT_FOR_DATA) {
                char* no_message = "No message.";
                char* no_message_ptr = malloc(strlen(no_message) + 1);
                strcpy(no_message_ptr, no_message);
                send_message.payload = no_message_ptr;
                send_message.status = OK_DONE;
            }

            communicate_with_client(client_socket, send_message);

            free(send_message.payload);
        }
    } while (!done);

    log_info("Connection closed at socket %d!\n", client_socket);
    client_context_free(client_context);
    close(client_socket);
}

/**
 * setup_server()
 *
 * This sets up the connection on the server side using unix sockets.
 * Returns a valid server socket fd on success, else -1 on failure.
 **/
int setup_server() {
    int server_socket;
    size_t len;
    struct sockaddr_un local;

    log_info("Attempting to setup server...\n");

    if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    local.sun_family = AF_UNIX;
    strncpy(local.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    unlink(local.sun_path);

    /*
    int on = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0)
    {
        log_err("L%d: Failed to set socket as reusable.\n", __LINE__);
        return -1;
    }
    */

    len = strlen(local.sun_path) + sizeof(local.sun_family) + 1;
    if (bind(server_socket, (struct sockaddr *)&local, len) == -1) {
        log_err("L%d: Socket failed to bind.\n", __LINE__);
        return -1;
    }

    if (listen(server_socket, 5) == -1) {
        log_err("L%d: Failed to listen on socket.\n", __LINE__);
        return -1;
    }

    return server_socket;
}

// Currently this main will setup the socket and accept a single client.
// After handling the client, it will exit.
// You will need to extend this to handle multiple concurrent clients
// and remain running until it receives a shut-down command.
int main(void)
{
    int server_socket = setup_server();
    if (server_socket < 0) {
        exit(1);
    }

    while (keep_server_alive) {
        log_info("Waiting for a connection %d ...\n", server_socket);

        struct sockaddr_un remote;
        socklen_t t = sizeof(remote);
        int client_socket = 0;

        if ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) == -1) {
            log_err("L%d: Failed to accept a new connection.\n", __LINE__);
            exit(1);
        }

        handle_client(client_socket);
    }

    return 0;
}
