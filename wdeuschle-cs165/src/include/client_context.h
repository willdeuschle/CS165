#ifndef CLIENT_CONTEXT_H
#define CLIENT_CONTEXT_H

#include "cs165_api.h"

Table* lookup_table(char *name);
Column* lookup_column(char *table_name, char* column_name);
/*
 * This function returns the index of a column in a table given a table name
 * and column name. Returns -1 if it doesn't exist.
 */
int lookup_column_idx(char* table_name, char* column_name);
Result* lookup_handle_result(char* handle, ClientContext* context);
void add_to_client_context(ClientContext* context, GeneralizedColumnHandle result_wrapper);

#endif
