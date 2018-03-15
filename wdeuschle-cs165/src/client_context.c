#include <string.h>
#include "client_context.h"
#include "utils.h"

/*
 * Adds a reference to a GeneralizedColumnHandle to the client context
 */
void add_to_client_context(ClientContext* context, GeneralizedColumnHandle result_wrapper) {
    // double size if necessary before inserting
    if (context->chandles_in_use == context->chandle_slots) {
        log_info("Resizing client context\n");
        // get reference to old table
        GeneralizedColumnHandle* old_handles = context->chandle_table;
        int old_size = context->chandle_slots;
        // double in size
        context->chandle_slots = 2 * old_size;
        context->chandle_table = malloc(context->chandle_slots * sizeof(GeneralizedColumnHandle));
        // copy over old information
        memcpy(context->chandle_table, old_handles, (sizeof(GeneralizedColumnHandle) * old_size));
        // free old table
        free(old_handles);
    }
    // replace any value currently in the client context if it has the
    // same handle, otherwise add to the end
    char* handle = result_wrapper.name;
    int current_num_items = context->chandles_in_use;
    int duplicate = -1;
    for (int i = 0; i < current_num_items; ++i) {
        if (strlen(context->chandle_table[i].name) == strlen(handle) && 
            strncmp(context->chandle_table[i].name, handle, strlen(handle)) == 0) {
            // first need to free this old result
            if (context->chandle_table[i].generalized_column.column_type == RESULT) {
                free(context->chandle_table[i].generalized_column.column_pointer.result->payload);
                free(context->chandle_table[i].generalized_column.column_pointer.result);
            } else {
                free(context->chandle_table[i].generalized_column.column_pointer.column->data);
                free(context->chandle_table[i].generalized_column.column_pointer.column);
            }
            // now overwrite it
            context->chandle_table[i] = result_wrapper;
            duplicate = 0;
        }
    }
    if (duplicate == -1) {
        // add the result wrapper to the end of the context array
        context->chandle_table[context->chandles_in_use++] = result_wrapper;
    }
    return;
}

/* This is an example of a function you will need to
 * implement in your catalogue. It takes in a string (char *)
 * and outputs a pointer to a table object. Similar methods
 * will be needed for columns and databases. How you choose
 * to implement the method is up to you.
 */
Table* lookup_table(char *name) {
    // TODO: implement a hash table for this?
    Table* table = NULL;
    // don't perform if we don't have a current_db
    if (current_db == NULL) {
        log_info("current_db does not exist\n");
        return table;
    }
    for (int i = 0; i < (int) current_db->tables_size; ++i) {
        if (strcmp(current_db->tables[i].name, name) == 0 && strlen(current_db->tables[i].name) == strlen(name)) {
            table = &(current_db->tables[i]);
            break;
        }
    }
	return table;
}

/*
 * This function returns a pointer to a column object given the table
 * and column name.
 */
Column* lookup_column(char* table_name, char* column_name) {
    Table* table = lookup_table(table_name);
    if (table == NULL) {
        return NULL;
    }
    Column* column = NULL;
    for (int i = 0; i < (int) table->col_size; ++i) {
        if (strcmp(table->columns[i].name, column_name) == 0) {
            column = &(table->columns[i]);
            break;
        }
    }
	return column;
}

/*
 * This function returns the index of a column in a table given a table name
 * and column name. Returns -1 if it doesn't exist.
 */
int lookup_column_idx(char* table_name, char* column_name) {
    Table* table = lookup_table(table_name);
    if (table == NULL) {
        return -1;
    }
    int column_idx = -1;
    for (int i = 0; i < (int) table->col_size; ++i) {
        if (strcmp(table->columns[i].name, column_name) == 0) {
            column_idx = i;
            break;
        }
    }
	return column_idx;
}

/*
 * This function returns a pointer to the result object stored under a 
 * given handle name for a given client context,
 */
Result* lookup_handle_result(char* handle, ClientContext* context) {
    Result* ids_result = NULL;
    int num_handles = context->chandles_in_use;
    // check the name of each current handle
    for (int i = 0; i < num_handles; ++i) {
        // TODO: more checks against the result used here?
        // compare names
        if (strcmp(handle, context->chandle_table[i].name) == 0) {
            ids_result = context->chandle_table[i].generalized_column.column_pointer.result;
            break;
        }
    }
    return ids_result;
}
