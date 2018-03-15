#ifndef DB_MANAGER_H
#define DB_MANAGER_H

#include "cs165_api.h"

/* 
 * This method accepts a dbo and creates an index on a column.
 * It modifies a send_message object to provide information about the operation.
 */
void create_index(DbOperator* dbo, message* send_message);

/* 
 * This method accepts a dbo and creates a database column from that information.
 * It modifies a send_message object to provide information about the operation.
 */
void create_column(DbOperator* dbo, message* send_message);

/* 
 * This method does the actual column creation and can be executed from other
 * locations (such as when we are loading a db).
 */
Column* create_column_helper(Table* table, char* name, int index_type, bool clustered);

/* 
 * This method accepts a dbo and creates a database table from that information.
 * It modifies a send_message object to provide information about the operation.
 */
void create_table(DbOperator* dbo, message* send_message);

/* 
 * This method does the actual table creation and can be executed from other
 * locations (such as when we are loading a db).
 */
Table* create_table_helper(char* name, size_t col_count, size_t table_capacity);

/* 
 * This method accepts a dbo and creates a database from that information.
 * It modifies a send_message object to provide information about the operation.
 */
void create_db(DbOperator* dbo, message* send_message);

/* 
 * This method loads columns into an existing database.
 */
Status load(const char* file_name);

/* 
 * Safely shut the database down. Persist all data to disk so that it can be
 * reloaded properly.
 */
Status shutdown_database(Db* db);

/* 
 * Safely shut the database down. Persist all data to disk as binary 
 * so that it can be reloaded properly.
 */
Status shutdown_database_binary(Db* db);

/* 
 * This method loads a database from disk. NOTE: this is for databases that
 * were created and then shutdown properly. Loading of columns is handled
 * by the load function.
 */
Status db_load(const char* file_name);

/* 
 * This method loads a binary database from disk. NOTE: this is for databases 
 * that were created and then shutdown properly. Loading of columns is handled
 * by the load function.
 */
Status db_load_binary(const char* file_name);

#endif /* DB_MANAGER_H */
