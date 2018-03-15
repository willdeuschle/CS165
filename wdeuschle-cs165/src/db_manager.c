#define _BSD_SOURCE
#include <string.h>
#include <stdbool.h>
#include <assert.h>
#include "cs165_api.h"
#include "db_updates.h"
#include "utils.h"
#include "client_context.h"
#include "btree.h"
#include "db_manager.h"


// Initially limit table length to 1000 items
// TODO: resize dynamically
#define STARTING_TABLE_CAPACITY 4096
// TODO: need to do this a better way
#define BUFFER_SIZE 4096

// In this class, there will always be only one active database at a time
Db *current_db = NULL;


/* 
 * This method accepts a dbo and creates an index on a column.
 * It modifies a send_message object to provide information about the operation.
 */
void create_index(DbOperator* dbo, message* send_message) {
    Column* column = dbo->operator_fields.create_index_operator.column;
    IndexType index_type = dbo->operator_fields.create_index_operator.index_type;
    bool clustered = dbo->operator_fields.create_index_operator.clustered;

    // update the index_type on this column
    column->index_type = index_type;

    // update the cluster type on this column
    column->clustered = clustered;

    // if this is a clustered btree, we need to create the auxilliary structure
    if (column->clustered) {
        // clustered index
        // if this is a btree index, create the auxilliary btree structure
        if (column->index_type == BTREE) {
            log_info("CREATING BTREE INDEX\n");
            BTree* btree;
            // false because clustered
            btree_init(&btree, false);
            column->index = btree;
        }
        // nothing else special to do here, just insert with respect to the index
    } else {
        // unclustered index
        // need to create a new column to handle the unclustered data
        // will be its own free standing data
        if (column->index_type == BTREE) {
            log_info("CREATING BTREE INDEX\n");
            BTree* btree;
            // true because unclustered, need the pos
            btree_init(&btree, true);
            column->index = btree;
        } else {
            // just use the STARTING_TABLE_CAPACITY because we haven't actually
            // inserted anything into this table yet if we are creating the index
            int* unclustered_column = malloc(sizeof(DataEntry) * STARTING_TABLE_CAPACITY);
            column->index = unclustered_column;
        }
    }

    const char* result_message = "index created";
    char* result = malloc(strlen(result_message) + 1);
    strcpy(result, result_message);
    send_message->payload = result;
    send_message->status = OK_DONE;
    return;
}

/* 
 * This method does the actual column creation and can be executed from other
 * locations (such as when we are loading a db).
 */
Column* create_column_helper(Table* table, char* name, int index_type, bool clustered) {
    // access the next column and increment the number of columns
    Column* col = &(table->columns[table->col_size++]);

    strcpy(col->name, name); // assign column name
    col->data = malloc(table->table_capacity * sizeof(int)); // initialize data pointer TODO: dynamically manage size of this data
    col->clustered = clustered;
    col->index_type = index_type;
    if (col->index_type == SORTED) {
        if (col->clustered) {
            log_info("CREATE SORTED, CLUSTERED INDEX\n");
            // nothing special to do here, we just use the base columns
        } else {
            log_err("CREATE SORTED, UNCLUSTERED INDEX\n");
            // allocate as much as we've already allocated for the base data
            DataEntry* clustered_column = malloc(table->table_capacity * sizeof(DataEntry));
            col->index = clustered_column;
        }
    } else if (col->index_type == BTREE) {
        if (col->clustered) {
            log_err("CREATE BTREE, CLUSTERED INDEX\n");
            BTree* btree;
            // false because clustered
            btree_init(&btree, false);
            col->index = btree;
        } else {
            log_info("CREATE BTREE, UNCLUSTERED INDEX\n");
            BTree* btree;
            // true because unclustered
            btree_init(&btree, true);
            col->index = btree;
        }
    }
    return col;
}

/* 
 * This method accepts a dbo and creates a database column from that information.
 * It modifies a send_message object to provide information about the operation.
 */
void create_column(DbOperator* dbo, message* send_message) {
    // creating a new db column
    log_info("creating new column: %s in table: %s\n", dbo->operator_fields.create_column_operator.name, dbo->operator_fields.create_column_operator.table_name);

    if (!current_db || strcmp(current_db->name, dbo->operator_fields.create_column_operator.db_name) != 0) {
        // try to load it
        if (db_load(dbo->operator_fields.create_column_operator.db_name).code == ERROR) {
            log_info("query unsupported. Bad db name\n");
            send_message->status = QUERY_UNSUPPORTED;
            return;
        }
    }

    // check that the table argument is a part of the current database
    Table* table = lookup_table(dbo->operator_fields.create_column_operator.table_name);
    if (table == NULL) {
        log_info("query unsupported. Bad db name\n");
        send_message->status = QUERY_UNSUPPORTED;
        return;
    }

    // check that we still have available columns
    if (table->col_size >= table->col_capacity) {
        log_info("query unsupported. Too many columns\n");
        send_message->status = QUERY_UNSUPPORTED;
        return;
    }

    // if we're creating a column this way, no index
    create_column_helper(table, dbo->operator_fields.create_column_operator.name, NO_INDEX, false);

    const char* result_message = "column created";
    char* result = malloc(strlen(result_message) + 1);
    strcpy(result, result_message);
    send_message->payload = result;
    send_message->status = OK_DONE;
	return;
}

/* 
 * This method does the actual table creation and can be executed from other
 * locations (such as when we are loading a db).
 */
Table* create_table_helper(char* name, size_t col_count, size_t table_capacity) {
    // access the next table and increment number of tables
    Table* table = &(current_db->tables[current_db->tables_size++]);

    strcpy(table->name, name); // assign table name
    table->col_capacity = col_count; // assign number of columns
    table->col_size = 0; // currently no columns created
    table->table_capacity = table_capacity; // max table capacity TODO: dynamically resize
    table->table_size = 0; // currently columns are empty, assign size of 0
    table->columns = malloc(col_count * sizeof(Column)); // initialize space for table columns
    memset(table->columns, 0, col_count * sizeof(Column));
    
    return table;
}

/* 
 * This method accepts a dbo and creates a database table from that information.
 * It modifies a send_message object to provide information about the operation.
 */
void create_table(DbOperator* dbo, message* send_message) {
    // TODO: handle max tables for db
    // creating a new db table
    log_info("creating new table\n");

    // check that the database argument is the current active database
    if (!current_db || strcmp(current_db->name, dbo->operator_fields.create_table_operator.db_name) != 0) {
        // try to load it
        if (db_load(dbo->operator_fields.create_table_operator.db_name).code == ERROR) {
            log_info("query unsupported. Bad db name\n");
            send_message->status = QUERY_UNSUPPORTED;
            return;
        }
    }

    create_table_helper(dbo->operator_fields.create_table_operator.name, dbo->operator_fields.create_table_operator.col_count, STARTING_TABLE_CAPACITY);

    const char* result_message = "table created";
    char* result = malloc(strlen(result_message) + 1);
    strcpy(result, result_message);
    send_message->payload = result;
    send_message->status = OK_DONE;
	return;
}

/* 
 * This method accepts a dbo and creates a database from that information.
 * It modifies a send_message object to provide information about the operation.
 */
void create_db(DbOperator* dbo, message* send_message) {
    // TODO: handle currently open db if we try to create a new one?

    // creating a new db
    log_info("creating new db\n");

    // initialize new db object
    Db* db = malloc(sizeof(Db)); // allocate space
    memset(db, 0, sizeof(Db));
    strcpy(db->name, dbo->operator_fields.create_db_operator.name); // assign db_name
    db->tables_capacity = 10; // choosing an initial capacity of 10 tables
    db->tables = malloc(db->tables_capacity * sizeof(Table)); // initialize space for db tables
    memset(db->tables, 0, db->tables_capacity * sizeof(Table));
    db->tables_size = 0; // no tables initially

    // set the current_db to this object
    current_db = db;
	
    const char* result_message = "db created";
    char* result = malloc(strlen(result_message) + 1);
    strcpy(result, result_message);
    send_message->payload = result;
    send_message->status = OK_DONE;
	return;
}

/* 
 * This method loads columns into an existing database.
 */
Status load(const char* file_name) {
	struct Status ret_status;
    log_info("loading file: %s\n", file_name);

    FILE* fp = NULL;
    
    // open file and validate success
    fp = fopen(file_name, "r");
    if (fp == NULL) {
        log_info("failed to open file");
        ret_status.code = ERROR;
        return ret_status;
    }

    int buffer_size = BUFFER_SIZE;
    // buffer to read data from the file
    char* line = malloc(buffer_size * sizeof(char));
    char* to_free = line;

    // read first line to get the list of columns
    if (fgets(line, buffer_size, fp) == NULL) {
        // close file
        fclose(fp);
        ret_status.code = ERROR;
        free(to_free);
        return ret_status;
    } else {
        // make a copy of the pointer to line - going to be mutating
        char* modifiable_line = line;

        // get the first db.table.col entry
        // TODO: verify the columns exist, make sure they are in the right order?
        message_status status = OK_DONE;
        char* first_col = next_token(&modifiable_line, &status);

        // error handling
        if (first_col == NULL) {
            // close file
            fclose(fp);
            ret_status.code = ERROR;
            free(to_free);
            return ret_status;
        }

        // parse the table -> first_col looks like "db.table.col" right now
        // move the first_col pointer to "table.col"
        split_on_period(&first_col, &status);
        // move the first_col pointer to "col", return pointer to "table"
        char* table_name = split_on_period(&first_col, &status);

        log_info("table name: %s\n", table_name);

        // check that the table argument is a part of the current database
        Table* table = lookup_table(table_name);
        if (table == NULL) {
            log_info("could not find table\n");
            // close file
            fclose(fp);
            ret_status.code = ERROR;
            free(to_free);
            return ret_status;
        }

        // allocate integer array with enough space for each column in
        // this table
        int* input_values = malloc(table->col_capacity * sizeof(int));
        // track the next integer value
        char* next_value = NULL;

        // for every data entry, insert into the database
        while (fgets(line, buffer_size, fp) != NULL) {
            if (table->table_size % 100 == 0)
                log_info("loading row\n");
            // going to be mutating, get a fresh pointer
            modifiable_line = line;
            if (table->table_size % 100 == 0)
                log_info("below modifiable line\n");
            // pointer to modifiable_line, in order to separate on the commas
            char** line_ptr = &modifiable_line;
            if (table->table_size % 100 == 0)
                log_info("below line ptr\n");
            // track the current entry number
            int columns_inserted = 0;
            if (table->table_size % 100 == 0)
                log_info("below columns inserted\n");
            // parse this input row, turning each entry into an integer
            while ((next_value = strsep(line_ptr, ",")) != NULL) {
                if (columns_inserted == 0 && table->table_size % 100 == 0) {
                    log_info("column 1 value: %s\n", next_value);
                }
                int insert_val = atoi(next_value);
                input_values[columns_inserted] = insert_val;
                columns_inserted++;
            }
            // insert this row
            if (table->table_size % 100 == 0)
                log_info("inserting row\n");
            insert_row(table, input_values);
            if (table->table_size % 100 == 0)
                log_info("below insert row\n");
        }

        log_info("finished reading from file");
        free(input_values);
    }


    // close file
    fclose(fp);

	ret_status.code = OK;
    free(to_free);
    log_info("returning from load");
	return ret_status;
}

/* 
 * This method loads a database from disk. NOTE: this is for databases that
 * were created and then shutdown properly. Loading of columns is handled
 * by the load function.
 */
Status db_load(const char* file_name) {
    // TODO: handle currently open db if we try to load another?
	struct Status ret_status;
    // switch to binary if we've flagged as such
    if (BINARY_DB == 1) {
        ret_status = db_load_binary(file_name);
        return ret_status;
    }
    
    // loading a db
    log_info("loading db\n");

    // want to load a file named for this database, add 4 for the .txt extension
    char db_file_name[HANDLE_MAX_SIZE + 4];
    strcpy(db_file_name, file_name);
    // add the .txt
    strcpy(db_file_name + strlen(file_name), ".txt\0");

    FILE* fp = NULL;
    
    // open file and validate success
    fp = fopen(db_file_name, "r");
    if (fp == NULL) {
        ret_status.code = ERROR;
        return ret_status;
    }


    // initialize new db object
    Db* db = malloc(sizeof(Db)); // allocate space
    // assign our current db to this
    current_db = db;


    // buffer to hold header data from the file
    char* header_buffer = malloc(BUFFER_SIZE * sizeof(char));
    char* header_to_free = header_buffer;
    // buffer to hold the data itself
    char* data_to_free = NULL;

    // read first line to get the db name, table_size, table_capacity
    // TODO: verify rest of input after this line
    if (fgets(header_buffer, BUFFER_SIZE, fp) == NULL) {
        // close file
        fclose(fp);
        ret_status.code = ERROR;
        free(header_to_free);
        return ret_status;
    } else {
        // db_name, tables_size, tables_capacity
        char* db_name = strsep(&header_buffer, ",");
        size_t tables_size = (size_t) atoi(strsep(&header_buffer, ","));
        size_t tables_capacity = (size_t) atoi(header_buffer);
        log_info("db_name: %s, tables_size: %zu, tables_capacity: %zu\n", db_name, tables_size, tables_capacity);
        // add to our db, initialize space
        strcpy(db->name, db_name);
        // we know the tables_size, but we'll increment this value as we create tables
        db->tables_size = 0;
        db->tables_capacity = tables_capacity;
        db->tables = malloc(db->tables_capacity * sizeof(Table)); // initialize space for db tables

        // for each table
        for (size_t i = 0; i < tables_size; ++i) {
            fgets(header_buffer, BUFFER_SIZE, fp); // pull in table descriptor line
            char* table_info = header_buffer;
            // name, col_size, col_capacity, tables_size, table_capacity
            char* table_name = strsep(&table_info, ",");
            size_t col_size = (size_t) atoi(strsep(&table_info, ","));
            size_t col_capacity = (size_t) atoi(strsep(&table_info, ","));
            size_t table_size = (size_t) atoi(strsep(&table_info, ","));
            size_t table_capacity = (size_t) atoi(strsep(&table_info, ","));
            log_info("table_name: %s, col_size: %zu, col_capacity: %zu, table_size: %zu, table_capacity: %zu\n", table_name, col_size, col_capacity, table_size, table_capacity);
            create_table_helper(table_name, col_capacity, table_capacity);
            db->tables[i].table_size = table_size;
            db->tables[i].table_capacity = table_capacity;
            // we know the col_size, but we'll increment this value as we create columns
            db->tables[i].col_size = 0;
            // compute the buffer size for each column
            // signed ints can be 11 characters max, plus an extra one for
            // a comma. also 1 for the null terminator
            size_t data_buffer_size = table_capacity * (12 + 1) * sizeof(char);
            char* data_buffer = malloc(data_buffer_size);
            data_to_free = data_buffer;
            // for each column in this table
            for (size_t j = 0; j < col_size; ++j) {
                fgets(header_buffer, BUFFER_SIZE, fp); // pull in name of the column
                char* column_info = header_buffer;
                char* col_name = strsep(&column_info, ",");
                int index_type = (int) atoi(strsep(&column_info, ","));
                bool clustered = (bool) atoi(strsep(&column_info, ","));
                log_info("column_name: %s, index_type: %d, clustered: %d\n", col_name, index_type, clustered);
                create_column_helper(&(db->tables[i]), col_name, index_type, clustered);
                // load in data
                fgets(data_buffer, data_buffer_size, fp); // pull in the data for this column
                char* string_data = data_buffer;
                for (size_t k = 0; k < table_size - 1; ++k) {
                    int next_data_pt = atoi(strsep(&string_data, ","));
                    if (k % 10000 == 0) {
                        log_info("data item #%zu: %d\n", k, next_data_pt);
                    }
                    db->tables[i].columns[j].data[k] = next_data_pt;
                    // account for btree
                    if (db->tables[i].columns[j].index_type == BTREE) {
                        if (db->tables[i].columns[j].clustered) {
                            // clustered: NOTE: passing false to all these btree_inserts because we are 
                            // no longer using this load function so it's not worth considering if they need them
                            btree_insert((BTree*) db->tables[i].columns[j].index, next_data_pt, CLUSTERED, false);
                        } else {
                            // unclustered
                            btree_insert((BTree*) db->tables[i].columns[j].index, next_data_pt, k, false);
                        }
                    } else if (db->tables[i].columns[j].index_type == SORTED && !db->tables[i].columns[j].clustered) {
                        // account for unclustered sorted index
                        insert_into_sorted_index((DataEntry*) db->tables[i].columns[j].index, k, next_data_pt, k);
                    }
                }
                // add last data item
                int final_data_pt = atoi(string_data);
                db->tables[i].columns[j].data[table_size - 1] = final_data_pt;
                // account for btree
                if (db->tables[i].columns[j].index_type == BTREE) {
                    if (db->tables[i].columns[j].clustered) {
                        // clustered
                        btree_insert((BTree*) db->tables[i].columns[j].index, final_data_pt, CLUSTERED, false);
                    } else {
                        // unclustered
                        btree_insert((BTree*) db->tables[i].columns[j].index, final_data_pt, table_size - 1, false);
                    }
                } else if (db->tables[i].columns[j].index_type == SORTED && !db->tables[i].columns[j].clustered) {
                    // account for unclustered sorted index
                    insert_into_sorted_index((DataEntry*) db->tables[i].columns[j].index, table_size - 1, final_data_pt, table_size - 1);
                }
            }
            free(data_to_free);
        }
    }

    // set the current_db to this object
    current_db = db;
	
    // close file
    fclose(fp);

	ret_status.code = OK;
    free(header_to_free);
	return ret_status;
}

/* 
 * Safely shut the database down. Persist all data to disk so that it can be
 * reloaded properly.
 */
Status shutdown_database(Db* db) {
    struct Status ret_status;
    // switch to binary if we've flagged as such
    if (BINARY_DB == 1) {
        ret_status = shutdown_database_binary(db);
        return ret_status;
    }

    log_info("shutdown db\n");

    // no current db
    if (db == NULL) {
        log_info("no current db\n");
	    ret_status.code = ERROR;
	    return ret_status;
    }

    // want to write to a file named for this database, add 4 for the .txt extension
    char db_file_name[HANDLE_MAX_SIZE + 4];
    strcpy(db_file_name, db->name);
    // add the .txt
    strcpy(db_file_name + strlen(db->name), ".txt\0");

    // file
    FILE* fp;
    fp = fopen(db_file_name, "w+");

    // first line is for the db
    // db_name, tables_size, tables_capacity
    fprintf(fp, "%s,%zu,%zu\n", db->name, db->tables_size, db->tables_capacity);

    // for every table
    for (size_t i = 0; i < db->tables_size; ++i) {
        Table* t = db->tables + i;
        // name, col_size, col_capacity, tables_size, table_capacity
        fprintf(fp, "%s,%zu,%zu,%zu,%zu\n", t->name, t->col_size, t->col_capacity, t->table_size, t->table_capacity);
        // for every column
        for (size_t j = 0; j < t->col_size; ++j) {
            Column* c = t->columns + j;
            // name, index_type, clustered
            fprintf(fp, "%s,%d,%d\n", c->name, c->index_type, c->clustered);
            // print all the data
            if (t->table_size > 0) {
                for (size_t k = 0; k < t->table_size - 1; ++k) {
                    // print all numbers with comma after them
                    fprintf(fp, "%d,", c->data[k]);
                }
                // print last number
                fprintf(fp, "%d\n", c->data[t->table_size - 1]);
            }
        }
    }

    // close file
    fclose(fp);

    // now switch the server off
    keep_server_alive = false;

	ret_status.code = OK;
	return ret_status;
}

/* 
 * Safely shut the database down. Persist all data to disk as binary 
 * so that it can be reloaded properly.
 */
Status shutdown_database_binary(Db* db) {
    struct Status ret_status;
    log_info("shutdown db\n");

    // no current db
    if (db == NULL) {
        log_info("no current db\n");
	    ret_status.code = ERROR;
	    return ret_status;
    }

    // want to write to a file named for this database, add 4 for the .bin extension
    char db_file_name[HANDLE_MAX_SIZE + 4];
    strcpy(db_file_name, db->name);
    // add the .txt
    strcpy(db_file_name + strlen(db->name), ".bin\0");

    // file
    FILE* fp;
    fp = fopen(db_file_name, "wb");

    // first line is for the db structure itself
    fwrite(db, sizeof(Db), 1, fp);
    // db_name, tables_size, tables_capacity
    /*fprintf(fp, "%s,%zu,%zu\n", db->name, db->tables_size, db->tables_capacity);*/

    // for every table
    for (size_t i = 0; i < db->tables_size; ++i) {
        Table* t = db->tables + i;
        // now the table structure
        fwrite(t, sizeof(Table), 1, fp);
        // name, col_size, col_capacity, tables_size, table_capacity
        /*fprintf(fp, "%s,%zu,%zu,%zu,%zu\n", t->name, t->col_size, t->col_capacity, t->table_size, t->table_capacity);*/
        // for every column
        for (size_t j = 0; j < t->col_size; ++j) {
            Column* c = t->columns + j;
            // now the column structure
            fwrite(c, sizeof(Column), 1, fp);
            // now the column data
            fwrite(c->data, sizeof(int), t->table_size, fp);
            // now the index on that column if it exists
            if (c->index_type == SORTED) {
                if (!c->clustered) {
                    // write this the unclustered column directly
                    fwrite(c->index, sizeof(DataEntry), t->table_size, fp);
                }
            } else if (c->index_type == BTREE) {
                // doesn't matter if this is clustered or unclustered
                // get the btree
                BTree* btree = (BTree*) c->index;
                // write the higher level btree structure
                fwrite(btree, sizeof(BTree), 1, fp);
                // write in each of the levels
                for (int level = 0; level < btree->height; ++level) {
                    // get the leftmost node at that level
                    Node* current_node = btree->root;
                    for (int within_level = 0; within_level < level; ++within_level) {
                        current_node = current_node->payload.signposts[0].node_pointer;
                    }
                    // have the leftmost node now
                    // write every node at this level
                    while (current_node) {
                        fwrite(current_node, sizeof(Node), 1, fp);
                        current_node = current_node->next;
                    }
                }
            }
        }
    }

    // close file
    fclose(fp);

    // now switch the server off
    keep_server_alive = false;

	ret_status.code = OK;
	return ret_status;
}

/* 
 * This method loads a binary database from disk. NOTE: this is for databases 
 * that were created and then shutdown properly. Loading of columns is handled
 * by the load function.
 */
Status db_load_binary(const char* file_name) {
	struct Status ret_status;
    // TODO: handle currently open db if we try to load another?
    
    // loading a db
    log_info("loading db\n");

    // want to load a file named for this database, add 4 for the .bin extension
    char db_file_name[HANDLE_MAX_SIZE + 4];
    strcpy(db_file_name, file_name);
    // add the .bin
    strcpy(db_file_name + strlen(file_name), ".bin\0");

    FILE* fp = NULL;
    
    // open file and validate success
    fp = fopen(db_file_name, "rb");
    if (fp == NULL) {
        ret_status.code = ERROR;
        return ret_status;
    }


    // initialize new db object
    Db* db = malloc(sizeof(Db)); // allocate space
    // assign our current db to this
    current_db = db;

    // read in the database structure
    fread(db, sizeof(Db), 1, fp);

    // allocate space for the tables
    db->tables = malloc(sizeof(Table) * db->tables_capacity);

    // for every table
    for (size_t i = 0; i < db->tables_size; ++i) {
        // access that table pointer
        Table* t = db->tables + i;
        // load the table
        fread(t, sizeof(Table), 1, fp);
        // allocate space for that table's columns
        t->columns = malloc(sizeof(Column) * t->col_capacity);

        // for every column
        for (size_t j = 0; j < t->col_size; ++j) {
            // access that column pointer
            Column* c = t->columns + j;
            // load the column
            fread(c, sizeof(Column), 1, fp);
            // allocate space for that column's data
            c->data = malloc(sizeof(int) * t->table_capacity);
            // load the column data
            fread(c->data, sizeof(int), t->table_size, fp);
            // now the index on that column if it exists
            if (c->index_type == SORTED) {
                if (!c->clustered) {
                    // write this the clustered column directly
                    // allocate space for this clustered, sorted index
                    c->index = malloc(sizeof(DataEntry) * t->table_capacity);
                    // load the index data
                    fread(c->index, sizeof(DataEntry), t->table_size, fp);
                }
            } else if (c->index_type == BTREE) {
                // doesn't matter if this is clustered or unclustered
                // allocate space for the btree
                c->index = malloc(sizeof(BTree));
                BTree* btree = (BTree*) c->index;
                // load the btree
                fread(btree, sizeof(BTree), 1, fp);
                // keep track of the parent from the previous level
                Node* parent = NULL;
                // load in each of the levels
                for (int level = 0; level < btree->height; ++level) {
                    // if this is the root, treat it differently
                    if (level == 0) {
                        // this is the root
                        // allocate space for the root
                        btree->root = malloc(sizeof(Node));
                        // load in the root
                        fread(btree->root, sizeof(Node), 1, fp);
                        // preserve this node's "greater_than" node pointer if it has one, even
                        // thought it is meaningless, we want to know if we need to restore it
                        Node* greater_than_ptr = btree->root->greater_than;
                        // clear it's pointers
                        clear_node_pointers(btree->root);
                        // add back the greater than pointer
                        btree->root->greater_than = greater_than_ptr;
                        // set this as the parent in the previous level
                        parent = btree->root;
                    } else {
                        // get the far left parent node of this level
                        parent = btree->root;
                        for (int current_level = 1; current_level < level; ++current_level) {
                            parent = parent->payload.signposts[0].node_pointer;
                        }
                        // we have the first parent at this level
                        while (parent) {
                            for (int entry_num = 0; entry_num < parent->num_entries; ++entry_num) {
                                // allocate space for this parent's child node
                                parent->payload.signposts[entry_num].node_pointer = malloc(sizeof(Node));
                                // read in this node
                                fread(parent->payload.signposts[entry_num].node_pointer, sizeof(Node), 1, fp);
                                // can now safely access this node
                                Node* new_node = parent->payload.signposts[entry_num].node_pointer;
                                assert(new_node->parent_index == entry_num);
                                // preserve this node's "greater_than" node pointer if it has one, even
                                // thought it is meaningless, we want to know if we need to restore it
                                Node* greater_than_ptr = new_node->greater_than;
                                // clear its pointers
                                clear_node_pointers(new_node);
                                // add back the greater than pointer
                                new_node->greater_than = greater_than_ptr;
                                // set the parent
                                new_node->parent = parent;
                                // if this isn't the first or "greater_than" node, set the prev
                                // and set that prev's next
                                if (entry_num == 0) {
                                    // see if there is a prev node to be had from the previous parent
                                    if (new_node->parent->prev) {
                                        // get that parent->prev node's last node
                                        Node* prev_parent = new_node->parent->prev;
                                        if (prev_parent->greater_than) {
                                            new_node->prev = prev_parent->greater_than;
                                            prev_parent->greater_than->next = new_node;
                                        } else {
                                            new_node->prev = prev_parent->payload.signposts[prev_parent->num_entries - 1].node_pointer;
                                            prev_parent->payload.signposts[prev_parent->num_entries - 1].node_pointer->next = new_node;
                                        }
                                    }
                                } else {
                                    Node* prev = new_node->parent->payload.signposts[new_node->parent_index - 1].node_pointer;
                                    prev->next = new_node;
                                    new_node->prev = prev;
                                }
                            }

                            if (parent->greater_than) {
                                // allocate space for the greater than
                                parent->greater_than = malloc(sizeof(Node));
                                // read in that node
                                fread(parent->greater_than, sizeof(Node), 1, fp);
                                // preserve this node's "greater_than" node pointer if it has one, even
                                // thought it is meaningless, we want to know if we need to restore it
                                Node* greater_than_ptr = parent->greater_than->greater_than;
                                // clear its pointers
                                clear_node_pointers(parent->greater_than);
                                // add back the greater than pointer
                                parent->greater_than->greater_than = greater_than_ptr;
                                // set the parent
                                parent->greater_than->parent = parent;
                                // set the prev
                                Node* prev = parent->payload.signposts[NUM_SIGNPOST_ENTRIES_PER_NODE - 1].node_pointer;
                                prev->next = parent->greater_than;
                                parent->greater_than->prev = prev;
                            }
                            // move on to the next parent
                            parent = parent->next;
                        }
                    }
                }
            }
        }
    }

    // close file
    fclose(fp);

	ret_status.code = OK;
	return ret_status;
}
