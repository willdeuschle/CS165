/* 
 * This file contains methods necessary to parse input from the client.
 * Mostly, functions in parse.c will take in string input and map these
 * strings into database operators. This will require checking that the
 * input from the client is in the correct format and maps to a valid
 * database operator.
 */

#define _BSD_SOURCE
#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
#include <assert.h>
#include "cs165_api.h"
#include "db_manager.h"
#include "parse.h"
#include "utils.h"
#include "client_context.h"
#include "batch_manager.h"
#include "db_updates.h"

#define MAX_NUM ~(1 << 31)
#define MIN_NUM 1 << 31
#define AVG_FLAG 'a'
#define SUM_FLAG 's'
#define MIN_FLAG 'l'
#define MAX_FLAG 'h'
#define ADD_FLAG 'a'
#define SUB_FLAG 's'
#define SHARED_QUERY_START_SIZE 16


// just declaring
int find_column_or_result(char* handle, GeneralizedColumn* to_compute, int* num_results, ClientContext* context, message* send_message);

/**
 * This method takes in a string representing the arguments to create an index
 * on a column.
 * It parses those arguments, checks that they are valid, and then creates
 * the index.
 **/

DbOperator* parse_create_idx(char* create_arguments, message* send_message) {
    // read and chop off last char, which should be a ')'
    int last_char = strlen(create_arguments) - 1;
    if (create_arguments[last_char] != ')') {
        // incorrect format
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
    // replace the ')' with a null terminating character. 
    create_arguments[last_char] = '\0';


    log_info("creating an index: %s\n", create_arguments);

    // structures to read the command into
    char db_tbl_col[HANDLE_MAX_SIZE];
    char index_type_str[HANDLE_MAX_SIZE];
    char clustered_str[HANDLE_MAX_SIZE];

    // scan in
    sscanf(create_arguments, "%[^,],%[^,],%[^,]", db_tbl_col, index_type_str, clustered_str);
    log_info("create index arguments: %s, %s, %s\n", db_tbl_col, index_type_str, clustered_str);

    // validate and convert these strings into useful information
    IndexType index_type;
    bool clustered;

    // we have a db.tbl.col right now
    // try to get a column and a table
    char* table_name = db_tbl_col;
    // returns the db_name, which we don't need
    split_on_period(&table_name, &send_message->status);
    char* column_name = table_name;
    // returns the table name, stores column name
    table_name = split_on_period(&column_name, &send_message->status);
    // now have the db_name, table_name, column_name
    // some validation
    if (send_message->status == INCORRECT_FORMAT) {
        log_err("incorrect format\n");
        return NULL;
    }

    // find the column and the table
    Table* table = lookup_table(table_name);
    Column* column = lookup_column(table_name, column_name);
    if (!table || !column) {
        send_message->status = OBJECT_NOT_FOUND;
        log_err("couldn't find table or column\n");
        return NULL;
    }

    // the index type
    if (strcmp(index_type_str, "sorted") == 0) {
        index_type = SORTED;
    } else if (strcmp(index_type_str, "btree") == 0) {
        index_type = BTREE;
    } else {
        // incorrect format
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }

    // clustered or not
    if (strcmp (clustered_str, "clustered") == 0) {
        clustered = true;
    } else if (strcmp(clustered_str, "unclustered") == 0) {
        clustered = false;
    } else {
        // incorrect format
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }

    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE_INDEX;
    dbo->operator_fields.create_index_operator.column = column;
    dbo->operator_fields.create_index_operator.index_type = index_type;
    dbo->operator_fields.create_index_operator.clustered = clustered;
    // update this indexed load for btrees if necessary
    if (index_type == BTREE) {
        btree_indexed_load = true;
        btree_indexed_table = table;
    }
    return dbo;
}


/**
 * This method takes in a string representing the arguments to create a column.
 * It parses those arguments, checks that they are valid, and creates a column.
 **/

DbOperator* parse_create_col(char* create_arguments, message* send_message) {
    DbOperator* dbo = NULL;

    send_message->status = OK_WAIT_FOR_RESPONSE;
    char** create_arguments_index = &create_arguments;
    char* col_name = next_token(create_arguments_index, &send_message->status);
    char* table_name = next_token(create_arguments_index, &send_message->status);

    // split the database and table
    char* db_name = split_on_period(&table_name, &send_message->status);

    log_info("db_name: %s\n", db_name);

    // not enough arguments or improperly formatted db and table
    if (send_message->status == INCORRECT_FORMAT) {
        return dbo;
    }

    // get the column name free of quotation marks
    col_name = trim_quotes(col_name);

    // read and chop off last char, which should be a ')'
    int last_char = strlen(table_name) - 1;
    if (table_name[last_char] != ')') {
        send_message->status = INCORRECT_FORMAT;
        return dbo;
    }
    // replace the ')' with a null terminating character. 
    table_name[last_char] = '\0';

    dbo = malloc(sizeof(DbOperator));
    strcpy(dbo->operator_fields.create_column_operator.db_name, db_name);
    strcpy(dbo->operator_fields.create_column_operator.table_name, table_name);
    strcpy(dbo->operator_fields.create_column_operator.name, col_name);
    dbo->type = CREATE_COLUMN;
    return dbo;
}

/**
 * This method takes in a string representing the arguments to create a table.
 * It parses those arguments, checks that they are valid, and creates a table.
 **/

DbOperator* parse_create_tbl(char* create_arguments, message* send_message) {
    DbOperator* dbo = NULL;

    send_message->status = OK_WAIT_FOR_RESPONSE;
    char** create_arguments_index = &create_arguments;
    char* table_name = next_token(create_arguments_index, &send_message->status);
    char* db_name = next_token(create_arguments_index, &send_message->status);
    char* col_count_str = next_token(create_arguments_index, &send_message->status);

    // not enough arguments
    if (send_message->status == INCORRECT_FORMAT) {
        return dbo;
    }

    // Get the table name free of quotation marks
    table_name = trim_quotes(table_name);

    // read and chop off last char, which should be a ')'
    int last_char = strlen(col_count_str) - 1;
    if (col_count_str[last_char] != ')') {
        send_message->status = INCORRECT_FORMAT;
        return dbo;
    }
    // replace the ')' with a null terminating character. 
    col_count_str[last_char] = '\0';

    // turn the string column count into an integer, and check that the input is valid.
    int col_count = atoi(col_count_str);
    if (col_count < 1) {
        send_message->status = INCORRECT_FORMAT;
        return dbo;
    }

    dbo = malloc(sizeof(DbOperator));
    strcpy(dbo->operator_fields.create_table_operator.db_name, db_name);
    strcpy(dbo->operator_fields.create_table_operator.name, table_name);
    dbo->operator_fields.create_table_operator.col_count = col_count;
    dbo->type = CREATE_TABLE;
    return dbo;
}

/**
 * This method takes in a string representing the arguments to create a database.
 * It parses those arguments, checks that they are valid, and creates a database.
 **/

DbOperator* parse_create_db(char* create_arguments, message* send_message) {
    DbOperator* dbo = NULL;
    char *token;
    token = strsep(&create_arguments, ",");
    // not enough arguments if token is NULL
    if (token == NULL) {
        send_message->status = INCORRECT_FORMAT;
        return dbo;
    } else {
        // create the database with given name
        char* db_name = token;
        // trim quotes and check for finishing parenthesis.
        db_name = trim_quotes(db_name);
        int last_char = strlen(db_name) - 1;
        if (last_char < 0 || db_name[last_char] != ')') {
            send_message->status = INCORRECT_FORMAT;
            return dbo;
        }
        // replace final ')' with null-termination character.
        db_name[last_char] = '\0';

        token = strsep(&create_arguments, ",");
        if (token != NULL) {
            send_message->status = INCORRECT_FORMAT;
            return dbo;
        }

        dbo = malloc(sizeof(DbOperator));
        strcpy(dbo->operator_fields.create_db_operator.name, db_name);
        dbo->type = CREATE_DB;
        return dbo;
    }
}

/**
 * parse_print parses a print statement, writing to the corresponding
 * output file
 **/
DbOperator* parse_print(char* print_arguments, message* send_message, ClientContext* context) {
    // parse the handle we are trying to print
    // first element should be a paren
    if (strncmp(print_arguments, "(", 1) == 0) {
        // get the handle string
        print_arguments++;

        // read and chop off last char, which should be a ')'
        int last_char = strlen(print_arguments) - 1;
        if (print_arguments[last_char] != ')') {
            // incorrect format
            send_message->status = UNKNOWN_COMMAND;
            return NULL;
        }
        // replace the ')' with a null terminating character. 
        print_arguments[last_char] = '\0';
        log_info("print handles: %s\n", print_arguments);

        // calculate how many things we are printing, start with 1 since there
        // will be one more handle than commas
        int num_handles = 1;
        int print_args_len = strlen(print_arguments);
        for (int i = 0; i < print_args_len; ++i) {
            if (print_arguments[i] == ',')
                ++num_handles;
        }

        log_info("printing %d handles: %d\n", num_handles);

        // all our handles
        char handles[num_handles][HANDLE_MAX_SIZE];

        for (int i = 0; i < num_handles; ++i) {
            char* next_val = strsep(&print_arguments, ",");
            // copy the handles
            strcpy(handles[i], next_val);
        }

        // print each handle
        for (int i = 0; i < num_handles; ++i) {
            log_info("handle to print: %s\n", handles[i]);
        }

        // create the print dbo
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = PRINT;
        dbo->operator_fields.print_operator.num_results = num_handles;
        dbo->operator_fields.print_operator.results = malloc(sizeof(void*) * num_handles);

        // get each handle
        for (int i = 0; i < num_handles; ++i) {
            Result* print_result = lookup_handle_result(handles[i], context);
            if (print_result == NULL) {
                log_info("handle not found: %s\n", handles[i]);
                // no handle
                send_message->status = OBJECT_NOT_FOUND;
                return NULL;
            } else {
                dbo->operator_fields.print_operator.results[i] = print_result;
            }
        }
        return dbo;
    } else {
        // incorrect format
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

/**
 * parse_create parses a create statement and then passes the necessary arguments off to the next function
 **/
DbOperator* parse_create(char* create_arguments, message* send_message) {
    DbOperator* dbo = NULL;
    send_message->status = OK_WAIT_FOR_RESPONSE;
    char *tokenizer_copy, *to_free;
    // Since strsep destroys input, we create a copy of our input. 
    tokenizer_copy = to_free = malloc((strlen(create_arguments)+1) * sizeof(char));
    char *token;
    strcpy(tokenizer_copy, create_arguments);
    // check for leading parenthesis after create. 
    if (strncmp(tokenizer_copy, "(", 1) == 0) {
        tokenizer_copy++;
        // token stores first argument. Tokenizer copy now points to just past first ","
        token = next_token(&tokenizer_copy, &send_message->status);
        if (send_message->status == INCORRECT_FORMAT) {
            free(to_free);
            return dbo;
        } else {
            // pass off to next parse function. 
            if (strcmp(token, "db") == 0) {
                dbo = parse_create_db(tokenizer_copy, send_message);
            } else if (strcmp(token, "tbl") == 0) {
                dbo = parse_create_tbl(tokenizer_copy, send_message);
            } else if (strcmp(token, "col") == 0) {
                dbo = parse_create_col(tokenizer_copy, send_message);
            } else if (strcmp(token, "idx") == 0) {
                dbo = parse_create_idx(tokenizer_copy, send_message);
            } else {
                send_message->status = UNKNOWN_COMMAND;
            }
        }
    } else {
        send_message->status = UNKNOWN_COMMAND;
    }
    free(to_free);
    return dbo;
}

/**
 * parse_fetch reads the arguments for a fetch statement and 
 * then passes these arguments to another function to execute. Also stores the
 * result in the client context.
 **/

DbOperator* parse_fetch(char* query_command, char* handle, message* send_message, ClientContext* context) {
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;

        // parse inputs

        // parse table input, still has the db attached
        char* table_name = next_token(command_index, &send_message->status);
        // split the database and table
        split_on_period(&table_name, &send_message->status);
        // table and column still attached
        char* column_name = table_name;
        // split the table and column
        table_name = split_on_period(&column_name, &send_message->status);
        log_info("table_name: %s, column_name: %s\n", table_name, column_name);
        // parse the ids used for this fetch
        char* ids_handle = next_token(command_index, &send_message->status);
        // read and chop off last char, which should be a ')'
        int last_char = strlen(ids_handle) - 1;
        if (ids_handle[last_char] != ')') {
            send_message->status = INCORRECT_FORMAT;
            return NULL;
        }
        // replace the ')' with a null terminating character. 
        ids_handle[last_char] = '\0';
        log_info("ids_handle: %s\n", ids_handle);

        // validate inputs
        
        if (send_message->status == INCORRECT_FORMAT) {
            log_err("incorrect format\n");
            return NULL;
        }
        // lookup the table and make sure it exists. 
        Table* fetch_table = lookup_table(table_name);
        if (fetch_table == NULL) {
            log_err("object not found 1\n");
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        // make sure our table isn't empty
        if (fetch_table->table_size == 0) {
            log_err("table emtpy\n");
            send_message->status = TABLE_EMPTY;
            return NULL;
        }
        log_err("what is table name %s, what is column_name %s\n", table_name, column_name);
        Column* fetch_column = lookup_column(table_name, column_name);
        if (fetch_column == NULL) {
            log_err("object not found 2\n");
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }

        // make sure we have this ids_handle
        Result* ids_result = lookup_handle_result(ids_handle, context);
        if (ids_result == NULL) {
            log_err("object not found 3\n");
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }

        // create the fetch dbo
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->operator_fields.fetch_operator.table = fetch_table;
        dbo->operator_fields.fetch_operator.ids_result = ids_result;
        dbo->operator_fields.fetch_operator.column = fetch_column;
        strcpy(dbo->operator_fields.fetch_operator.handle, handle);
        dbo->type = FETCH;
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

/**
 * parse_join reads the arguments for a join statement and 
 * then passes these arguments to another function to execute. Also stores the 
 * results in the client context.
 **/

DbOperator* parse_join(char* join_arguments, char* handle, message* send_message, ClientContext* context) {
    // get the left_handle and right_handle
    char left_handle[HANDLE_MAX_SIZE];
    char right_handle[HANDLE_MAX_SIZE];
    log_info("before %s\n", handle);
    sscanf(handle, "%[^,],%[^,]", left_handle, right_handle);
    log_info("left_handle: %s, right_handle: %s\n", left_handle, right_handle);
    
    // check for leading '('
    if (strncmp(join_arguments, "(", 1) == 0) {
        join_arguments++;

        // parse inputs

        // read and chop off last char, which should be a ')'
        int last_char = strlen(join_arguments) - 1;
        if (join_arguments[last_char] != ')') {
            // incorrect format
            send_message->status = UNKNOWN_COMMAND;
            return NULL;
        }
        // replace the ')' with a null terminating character. 
        join_arguments[last_char] = '\0';


        log_info("parsing a join: %s\n", join_arguments);

        // structures to read the command into
        char val1_str[HANDLE_MAX_SIZE];
        char pos1_str[HANDLE_MAX_SIZE];
        char val2_str[HANDLE_MAX_SIZE];
        char pos2_str[HANDLE_MAX_SIZE];
        char join_type_str[HANDLE_MAX_SIZE];
        int join_type;

        // scan in
        sscanf(join_arguments, "%[^,],%[^,],%[^,],%[^,],%[^,]", val1_str, pos1_str, val2_str, pos2_str, join_type_str);
        log_info("join arguments: %s, %s, %s, %s, %s\n", val1_str, pos1_str, val2_str, pos2_str, join_type_str);

        // get the join type
        if (strcmp(join_type_str, "nested-loop") == 0) {
            join_type = NESTED_LOOP_JOIN;
        } else if (strcmp(join_type_str, "hash") == 0) {
            join_type = HASH_JOIN;
        } else {
            // incorrect format
            log_err("Not a known type of join\n");
            send_message->status = UNKNOWN_COMMAND;
            return NULL;
        }

        // lookup the handles and make sure they exist
        Result* val1_result = lookup_handle_result(val1_str, context);
        Result* pos1_result = lookup_handle_result(pos1_str, context);
        Result* val2_result = lookup_handle_result(val2_str, context);
        Result* pos2_result = lookup_handle_result(pos2_str, context);
        if (val1_result == NULL || pos1_result == NULL ||
            val2_result == NULL || pos2_result == NULL) {
            log_err("Couldn't find one of the necessary positions or values\n");
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }

        // create the join dbo
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->operator_fields.join_operator.join_type = join_type;
        dbo->operator_fields.join_operator.val1_result = val1_result;
        dbo->operator_fields.join_operator.pos1_result = pos1_result;
        dbo->operator_fields.join_operator.val2_result = val2_result;
        dbo->operator_fields.join_operator.pos2_result = pos2_result;
        strcpy(dbo->operator_fields.join_operator.left_handle, left_handle);
        strcpy(dbo->operator_fields.join_operator.right_handle, right_handle);
        dbo->type = JOIN;
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

/**
 * find_column_or_result populates a Generalized column and num_results field
 * with the appropriate values based on a handle (either a column or a result)
 * returns 0 on success and -1 on failure
 **/
int find_column_or_result(char* handle, GeneralizedColumn* to_compute, int* num_results, ClientContext* context, message* send_message) {
    // check to see if it's a result
    Result* result = lookup_handle_result(handle, context);
    if (result == NULL) {
        log_info("looking for a column to compute over instead\n");
        // try to find a column instead
        // currently db.table.column
        char* table_name = handle;
        // returns the db_name
        char* db_name = split_on_period(&table_name, &send_message->status);

        // check that the database argument is the current active database
        if (!current_db || strcmp(current_db->name, db_name) != 0) {
            // try to load it
            if (db_load(db_name).code == ERROR) {
                log_err("Invalid db\n");
                return -1;
            }
        }

        char* column_name = table_name;
        table_name = split_on_period(&column_name, &send_message->status);

        // find the column
        Column* column = lookup_column(table_name, column_name);

        if (column == NULL) {
            // neither a column nor something in client context
            log_err("No result or column found\n");
            return -1;
        } else {
            // we found a column
            // set this to our generalized column
            to_compute->column_type = COLUMN;
            to_compute->column_pointer.column = column;
            // get the table so we know the column size
            Table* table = lookup_table(table_name);
            *num_results = table->table_size;
        }
    } else {
        // we found a result
        // set this to our generalized column
        to_compute->column_type = RESULT;
        to_compute->column_pointer.result = result;
        *num_results = result->num_tuples;
    }
    return 0;
}

/**
 * parse_add reads the arguments for an add operation and passes these on in 
 * the form of a DbOperator to another function to compute the result. Also 
 * stores the result in the client context.
 **/
DbOperator* parse_add(char* query_command, char* handle, message* send_message, ClientContext* context, char computation) {
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        char* ids_handle = query_command + 1;

        // parse input

        // should just be two handle names followed by a ")"

        // read and chop off last char, which should be a ')'
        int last_char = strlen(ids_handle) - 1;
        if (ids_handle[last_char] != ')') {
            send_message->status = INCORRECT_FORMAT;
            return NULL;
        }
        // replace the ')' with a null terminating character. 
        ids_handle[last_char] = '\0';
        log_info("result to compute over: %s\n", ids_handle);
        
        // get each handle
        char handle1[HANDLE_MAX_SIZE];
        char handle2[HANDLE_MAX_SIZE];

        // separate them
        sscanf(ids_handle, "%[^,],%[^,]", handle1, handle2);

        log_info("handle1: %s, handle2: %s\n", handle1, handle2);

        // lookup both handles
        
        // handle1
        GeneralizedColumn to_compute1;
        int num_results1;
        int success1 = find_column_or_result(handle1, &to_compute1, &num_results1, context, send_message);
        // handle2
        GeneralizedColumn to_compute2;
        int num_results2;
        int success2 = find_column_or_result(handle2, &to_compute2, &num_results2, context, send_message);

        if (success1 == -1 || success2 == -1) {
            log_err("failure to find both handles\n");
            send_message->status = INCORRECT_FORMAT;
            return NULL;
        }

        // create the correct dbo type
        DbOperator* dbo = malloc(sizeof(DbOperator));
        // first column
        dbo->operator_fields.add_operator.generalized_column1 = to_compute1;
        dbo->operator_fields.add_operator.num_results1 = num_results1;
        // second column
        dbo->operator_fields.add_operator.generalized_column2 = to_compute2;
        dbo->operator_fields.add_operator.num_results2 = num_results2;
        strcpy(dbo->operator_fields.add_operator.handle, handle);
        if (computation == ADD_FLAG) {
            dbo->type = ADD;
        } else {
            dbo->type = SUB;
        }
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}


/**
 * parse_aggregate reads the arguments for a sum or avg statement (they take 
 * the same args) and passes these on in the form of a DbOperator to another 
 * function to compute the result. Also stores the result in the client context.
 **/

DbOperator* parse_aggregate(char* query_command, char* handle, message* send_message, ClientContext* context, char computation) {
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        char* ids_handle = query_command + 1;

        // parse input

        // should just be a handle name followed by a ")"

        // read and chop off last char, which should be a ')'
        int last_char = strlen(ids_handle) - 1;
        if (ids_handle[last_char] != ')') {
            send_message->status = INCORRECT_FORMAT;
            return NULL;
        }
        // replace the ')' with a null terminating character. 
        ids_handle[last_char] = '\0';
        log_info("result to compute over: %s\n", ids_handle);

        // find the column we are averaging
        GeneralizedColumn to_compute;
        int num_results;
        // check to see if it's a handle
        Result* result = lookup_handle_result(ids_handle, context);
        if (result == NULL) {
            log_info("looking for a column to compute over instead\n");
            // try to find a column instead
            // currently db.table.column
            char* table_name = ids_handle;
            // returns the db_name, we're ignoring this
            split_on_period(&table_name, &send_message->status);
            char* column_name = table_name;
            table_name = split_on_period(&column_name, &send_message->status);

            // find the column
            Column* column = lookup_column(table_name, column_name);

            if (column == NULL) {
                // neither a column nor something in client context
                log_err("No result or column found\n");
                send_message->status = OBJECT_NOT_FOUND;
                return NULL;
            } else {
                // we found a column
                // set this to our generalized column
                to_compute.column_type = COLUMN;
                to_compute.column_pointer.column = column;
                // get the table so we know the column size
                Table* table = lookup_table(table_name);
                num_results = table->table_size;
            }
        } else {
            // we found a result
            // set this to our generalized column
            to_compute.column_type = RESULT;
            to_compute.column_pointer.result = result;
            num_results = result->num_tuples;
        }

        // create the correct dbo type
        DbOperator* dbo = malloc(sizeof(DbOperator));
        if (computation == AVG_FLAG) {
            dbo->operator_fields.average_operator.generalized_column = to_compute;
            dbo->operator_fields.average_operator.num_results = num_results;
            strcpy(dbo->operator_fields.average_operator.handle, handle);
            dbo->type = AVERAGE;
        } else if (computation == SUM_FLAG) {
            dbo->operator_fields.sum_operator.generalized_column = to_compute;
            dbo->operator_fields.sum_operator.num_results = num_results;
            strcpy(dbo->operator_fields.sum_operator.handle, handle);
            dbo->type = SUM;
        } else if (computation == MIN_FLAG) {
            dbo->operator_fields.min_operator.generalized_column = to_compute;
            dbo->operator_fields.min_operator.num_results = num_results;
            strcpy(dbo->operator_fields.min_operator.handle, handle);
            dbo->type = MIN;
        } else if (computation == MAX_FLAG) {
            dbo->operator_fields.max_operator.generalized_column = to_compute;
            dbo->operator_fields.max_operator.num_results = num_results;
            strcpy(dbo->operator_fields.max_operator.handle, handle);
            dbo->type = MAX;
        }
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

/**
 * parse_select reads in the arguments for a select statement and 
 * then passes these arguments to a database function to pull out the
 * relevant rows. Handle refers to the temporary result we are assign
 * the results to.
 **/

DbOperator* parse_select(char* query_command, char* handle, message* send_message, ClientContext* context) {
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command = query_command + 1;

        // parse input
        // should just be two handle names followed by a ")"

        // read and chop off last char, which should be a ')'
        int last_char = strlen(query_command) - 1;
        if (query_command[last_char] != ')') {
            send_message->status = INCORRECT_FORMAT;
            return NULL;
        }
        // replace the ')' with a null terminating character. 
        query_command[last_char] = '\0';
        log_info("result to compute over: %s\n", query_command);

        // get input information
        char arg1[HANDLE_MAX_SIZE];
        char arg2[HANDLE_MAX_SIZE];
        char arg3[HANDLE_MAX_SIZE];
        char arg4[HANDLE_MAX_SIZE];

        // information for our dbo
        char* posn_vector_handle = NULL;
        char* ids_handle;
        char* start_range_ptr;
        char* end_range_ptr;
        int start_range;
        int end_range;
        bool has_posn_vector = false;

        // separate them
        int num_args = sscanf(query_command, "%[^,],%[^,],%[^,],%s", arg1, arg2, arg3, arg4);
        
        if (num_args < 3 || num_args > 4) {
            log_err("failure to parse proper arguments\n");
            send_message->status = INCORRECT_FORMAT;
            return NULL;
        } else if (num_args == 3) {
            // 3 args, we just have a column and ranges
            ids_handle = arg1;
            start_range_ptr = arg2;
            end_range_ptr = arg3;
        } else {
            // 4 args, posn vector, column, ranges
            posn_vector_handle = arg1;
            ids_handle = arg2;
            start_range_ptr = arg3;
            end_range_ptr = arg4;
        }

        // access the positions vector (if it exists)
        GeneralizedColumn posn_vector;
        int num_posn_results;
        if (posn_vector_handle != NULL) {
            has_posn_vector = true;
            int pos_success = find_column_or_result(posn_vector_handle, &posn_vector, &num_posn_results, context, send_message);
            if (pos_success == -1) {
                log_err("failure to find ids_handle\n");
                send_message->status = OBJECT_NOT_FOUND;
                return NULL;
            }
        }

        // access the values vector
        GeneralizedColumn to_compute;
        int num_results;
        int success = find_column_or_result(ids_handle, &to_compute, &num_results, context, send_message);

        if (success == -1) {
            log_err("failure to find ids_handle\n");
            send_message->status = INCORRECT_FORMAT;
            return NULL;
        }

        // convert the range operators
        if (strcmp(start_range_ptr, "null") == 0) {
            // set to start_range MIN_NUM
            start_range = MIN_NUM;
        } else {
            start_range = atoi(start_range_ptr);
        }
        if (strcmp(end_range_ptr, "null") == 0) {
            // set to end_range to MAX_NUM
            end_range = MAX_NUM;
            log_info("null max: %d\n", end_range);
        } else {
            end_range = atoi(end_range_ptr);
        }

        // make select operator. 
        
        // create necessary wrapper around the column
        // create the select dbo
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->operator_fields.select_operator.compare_info = malloc(sizeof(Comparator));
        dbo->type = SELECT;
        strcpy(dbo->operator_fields.select_operator.compare_info->handle, handle);
        dbo->operator_fields.select_operator.compare_info->gen_col = to_compute;
        dbo->operator_fields.select_operator.compare_info->p_low = start_range;
        dbo->operator_fields.select_operator.compare_info->p_high = end_range;
        dbo->operator_fields.select_operator.num_results = num_results;
        dbo->operator_fields.select_operator.compare_info->has_posn_vector = has_posn_vector;

        // add posn vector if we are using that
        if (has_posn_vector == true) {
            dbo->operator_fields.select_operator.compare_info->posn_vector = posn_vector;
            dbo->operator_fields.select_operator.num_posn_results = num_posn_results;
        }


        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

/**
 * parse_insert reads in the arguments for a create statement and 
 * then passes these arguments to a database function to insert a row.
 **/

DbOperator* parse_insert(char* query_command, message* send_message) {
    unsigned int columns_inserted = 0;
    char* token = NULL;
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;

        // parse inputs
        
        // parse table input, still has the db attached
        char* table_name = next_token(command_index, &send_message->status);
        // split the database and table
        split_on_period(&table_name, &send_message->status);

        // validate inputs

        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }
        // lookup the table and make sure it exists. 
        Table* insert_table = lookup_table(table_name);
        if (insert_table == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        // make sure we have created the right number of columns for this table
        if (insert_table->col_capacity != insert_table->col_size) {
            send_message->status = INCOMPLETE_DATA;
            return NULL;
        }

        // make insert operator. 
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = INSERT;
        dbo->operator_fields.insert_operator.table = insert_table;
        dbo->operator_fields.insert_operator.values = malloc(sizeof(int) * insert_table->col_capacity);
        // parse inputs until we reach the end. Turn each given string into an integer. 
        while ((token = strsep(command_index, ",")) != NULL) {
            int insert_val = atoi(token);
            dbo->operator_fields.insert_operator.values[columns_inserted] = insert_val;
            columns_inserted++;
        }
        // check that we received the correct number of input values
        if (columns_inserted != insert_table->col_capacity) {
            send_message->status = INCORRECT_FORMAT;
            db_operator_free(dbo);
            return NULL;
        } 
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

/**
 * parse_update reads in the arguments for an update statement, creates a dbo,
 * then passes that dbo on to be executed
 **/

DbOperator* parse_update(char* query_command, message* send_message, ClientContext* context) {
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;

        // parse inputs

        // parse table input, still has the db attached
        char* table_name = next_token(command_index, &send_message->status);
        // split the database and table
        split_on_period(&table_name, &send_message->status);
        // table and column still attached
        char* column_name = table_name;
        // split the table and column
        table_name = split_on_period(&column_name, &send_message->status);
        log_info("table_name: %s, column_name: %s\n", table_name, column_name);

        // parse the updated ids handle
        char* id_handle = next_token(command_index, &send_message->status);

        // validate inputs
        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }

        // lookup this handle
        Result* handle = lookup_handle_result(id_handle, context);
        if (handle == NULL) {
            log_info("handle not found: %s\n", id_handle);
            // no handle
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        // should have found one result
        assert(handle->num_tuples == 1);

        // parse the new value for this update
        char* new_value_str = next_token(command_index, &send_message->status);

        // validate inputs
        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }

        // read and chop off last char, which should be a ')'
        int last_char = strlen(new_value_str) - 1;
        if (new_value_str[last_char] != ')') {
            send_message->status = INCORRECT_FORMAT;
            return NULL;
        }
        new_value_str[last_char] = '\n';

        // convert this value to an integer
        int new_value = atoi(new_value_str);
        
        // lookup the table and make sure it exists. 
        Table* table = lookup_table(table_name);
        if (table == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        // lookup the column and make sure it exists. 
        int column_idx = lookup_column_idx(table_name, column_name);
        if (column_idx == -1) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }

        // make update operator. 
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = UPDATE;
        dbo->operator_fields.update_operator.table = table;
        dbo->operator_fields.update_operator.column_idx = column_idx;
        dbo->operator_fields.update_operator.row_to_update = handle;
        dbo->operator_fields.update_operator.new_value = new_value;
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

/**
 * parse_delete reads in the arguments for an update statement, creates a dbo,
 * then passes that dbo on to be executed
 **/

DbOperator* parse_delete(char* query_command, message* send_message, ClientContext* context) {
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;

        // parse inputs

        // parse table input, still has the db attached
        char* table_name = next_token(command_index, &send_message->status);
        // split the database and table
        split_on_period(&table_name, &send_message->status);
        // parse the deleted ids handle
        char* id_handle = next_token(command_index, &send_message->status);

        // validate inputs
        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }

        // read and chop off last char, which should be a ')'
        int last_char = strlen(id_handle) - 1;
        if (id_handle[last_char] != ')') {
            send_message->status = INCORRECT_FORMAT;
            return NULL;
        }

        id_handle[last_char] = '\0';

        // lookup this handle
        Result* handle = lookup_handle_result(id_handle, context);
        if (handle == NULL) {
            log_info("handle not found: %s\n", id_handle);
            // no handle
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }

        // lookup the table and make sure it exists. 
        Table* table = lookup_table(table_name);
        if (table == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }

        // make delete operator. 
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = DELETE;
        dbo->operator_fields.delete_operator.table = table;
        dbo->operator_fields.delete_operator.row_to_delete = handle;
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

/**
 * parse_command takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 **/
DbOperator* parse_command(char* query_command, message* send_message, int client_socket, ClientContext* context) {
    DbOperator *dbo = NULL; // = malloc(sizeof(DbOperator)); // calloc?

    if (strncmp(query_command, "--", 2) == 0) {
        send_message->status = OK_DONE;
        // The -- signifies a comment line, no operator needed.  
        return NULL;
    }

    char *equals_pointer = strchr(query_command, '=');
    char *handle = query_command;
    if (equals_pointer != NULL) {
        // handle exists, store here. 
        *equals_pointer = '\0';
        log_info("FILE HANDLE: %s\n", handle);
        query_command = ++equals_pointer;
    } else {
        handle = NULL;
    }

    log_info("QUERY: %s\n", query_command);

    send_message->status = OK_WAIT_FOR_RESPONSE;
    query_command = trim_whitespace(query_command);

    // check what command is given. 
    if (strncmp(query_command, "batch_queries()", 15) == 0) {
        // set that we are now batching queries
        currently_batching_query = true;
        // allocate space for the dbos
        shared_scan_operators.dbo_slots = SHARED_QUERY_START_SIZE;
        shared_scan_operators.dbos = malloc(sizeof(DbOperator*) * SHARED_QUERY_START_SIZE);
        return NULL;
    } else if (strncmp(query_command, "finished_load", 13) == 0) {
        // we are no longer loading, mark our btree_indexed_load as false (may 
        // already be false, that's okay)
        btree_indexed_load = false;
        if (btree_indexed_table != NULL) {
            // update the index on that table here
            btree_update_for_indexed_table(btree_indexed_table);
            // no longer need to keep track of it
            btree_indexed_table = NULL;
        }
        printf("FINISHED LOAD\n");
        return NULL;
    } else if (strncmp(query_command, "batch_execute()", 15) == 0) {
        // execute the batch, also we are no longer batching
        dbo = construct_batch_operator();
        currently_batching_query = false;
    } else if (strncmp(query_command, "create", 6) == 0) {
        query_command += 6;
        dbo = parse_create(query_command, send_message);
    } else if (strncmp(query_command, "relational_insert", 17) == 0) {
        query_command += 17;
        dbo = parse_insert(query_command, send_message);
    } else if (strncmp(query_command, "relational_update", 17) == 0) {
        query_command += 17;
        dbo = parse_update(query_command, send_message, context);
    } else if (strncmp(query_command, "relational_delete", 17) == 0) {
        query_command += 17;
        dbo = parse_delete(query_command, send_message, context);
    } else if (strncmp(query_command, "select", 6) == 0) {
        query_command += 6;
        dbo = parse_select(query_command, handle, send_message, context);
    } else if (strncmp(query_command, "fetch", 5) == 0) {
        query_command += 5;
        dbo = parse_fetch(query_command, handle, send_message, context);
    } else if (strncmp(query_command, "join", 4) == 0) {
        query_command += 4;
        dbo = parse_join(query_command, handle, send_message, context);
    } else if (strncmp(query_command, "avg", 3) == 0) {
        query_command += 3;
        dbo = parse_aggregate(query_command, handle, send_message, context, AVG_FLAG);
    } else if (strncmp(query_command, "sum", 3) == 0) {
        query_command += 3;
        dbo = parse_aggregate(query_command, handle, send_message, context, SUM_FLAG);
    } else if (strncmp(query_command, "min", 3) == 0) {
        query_command += 3;
        dbo = parse_aggregate(query_command, handle, send_message, context, MIN_FLAG);
    } else if (strncmp(query_command, "max", 3) == 0) {
        query_command += 3;
        dbo = parse_aggregate(query_command, handle, send_message, context, MAX_FLAG);
    } else if (strncmp(query_command, "add", 3) == 0) {
        query_command += 3;
        dbo = parse_add(query_command, handle, send_message, context, ADD_FLAG);
    } else if (strncmp(query_command, "sub", 3) == 0) {
        query_command += 3;
        dbo = parse_add(query_command, handle, send_message, context, SUB_FLAG);
    } else if (strncmp(query_command, "shutdown", 8) == 0) {
        /*if (shutdown_database(current_db).code == OK) {*/
            /*send_message->status = OK_DONE;*/
        /*} else {*/
            /*send_message->status = EXECUTION_ERROR;*/
        /*}*/
        /*dbo = malloc(sizeof(DbOperator));*/
        /*dbo->type = SHUTDOWN;*/
    } else if (strncmp(query_command, "real_shutdown", 13) == 0) {
        // this is just a convenient hook for me while testing
        if (shutdown_database(current_db).code == OK) {
            send_message->status = OK_DONE;
        } else {
            send_message->status = EXECUTION_ERROR;
        }
        dbo = malloc(sizeof(DbOperator));
        dbo->type = SHUTDOWN;
    } else if (strncmp(query_command, "print", 5) == 0) {
        query_command += 5;
        dbo = parse_print(query_command, send_message, context);
    }
    if (dbo == NULL) {
        return dbo;
    }

    if (currently_batching_query == true && dbo->type != SHARED_SCAN) {
        log_info("log shared scan, dont execute yet\n");
        // dont send on the actual dbo, log it to be executed later and send
        // along a different one
        add_shared_dbo(dbo);
        DbOperator* new_dbo = malloc(sizeof(DbOperator));
        new_dbo->type = SHARED_QUERY_LOGGED;
        dbo = new_dbo;
    }
    
    dbo->client_fd = client_socket;
    dbo->context = context;
    return dbo;
}
