#ifndef DB_UPDATES_H
#define DB_UPDATES_H

#include "cs165_api.h"

/* 
 * this function inserts a row into a table
 */
void insert_row(Table* table, int* input_values);

/* 
 * this function deletes a row from a table and returns the deleted row  to
 * the caller
 */
int* delete_row(Table* table, Result* row_to_delete);

/* 
 * this function inserts a row into the database, and stores the result
 * message in send_messgae
 */
void db_insert(DbOperator* query, message* send_message);

/* 
 * this function handles a query to update a row into the database, and 
 * returns the result message
 */
void db_update(DbOperator* query, message* send_message);

/* 
 * this function handles a query to delete a row into the database, and 
 * returns the result message
 */
void db_delete(DbOperator* query, message* send_message);

/* 
 * this function inserts a value and position into a sorted column index
 */
void insert_into_sorted_index(DataEntry* data, int num_records, int value, int pos);

/* 
 * this function updates btree indexes after a table load
 */
void btree_update_for_indexed_table(Table* table);

#endif /* DB_UPDATES_H */
