

/* BREAK APART THIS API (TODO MYSELF) */
/* PLEASE UPPERCASE ALL THE STUCTS */

/*
Copyright (c) 2015 Harvard University - Data Systems Laboratory (DASLab)
Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#ifndef CS165_H
#define CS165_H

#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include "message.h"

// Limits the size of a name in our database to 64 characters
#define HANDLE_MAX_SIZE 64
#define BITVECTOR_DB 1
#define BINARY_DB 1
#define INT 1
#define LONG 2
#define DOUBLE 3
#define NESTED_LOOP_JOIN 1
#define HASH_JOIN 2

extern bool keep_server_alive;
// track whether we are loading a table with a btree index
extern bool btree_indexed_load;

/**
 * EXTRA
 * DataTyype
 * Flag to mark what type of data is held in the struct.
 * You can support additional types by including this enum and using void*
 * in place of int* in db_operator simliar to the way IndexType supports
 * additional types.
 * UPDATE - this was causing problems with forward declarations so I switched
 * it to being macros
 **/

// a data entry can either be a (value, pos) pair, or just the value itself
typedef struct DataEntry {
    int value;
    int pos;
} DataEntry;

typedef enum IndexType {
    NO_INDEX,
    SORTED,
    BTREE,
} IndexType;

struct Comparator;
//struct ColumnIndex;

typedef struct Column {
    char name[HANDLE_MAX_SIZE];
    int* data;
    // You will implement column indexes later. 
    void* index;
    //struct ColumnIndex *index;
    IndexType index_type;
    bool clustered;
} Column;


/**
 * table
 * Defines a table structure, which is composed of multiple columns.
 * We do not require you to dynamically manage the size of your tables,
 * although you are free to append to the struct if you would like to (i.e.,
 * include a size_t table_size).
 * name, the name associated with the table. table names must be unique
 *     within a database, but tables from different databases can have the same
 *     name.
 * - col_capacity, the number of columns in the table
 * - col_size, the current number of columns in the table
 * - columns, this is the pointer to an array of columns contained in the table.
 * - table_capacity, the capacity of the columns in the table.
 * - table_size, the current size of the columns in the table.
 **/

typedef struct Table {
    char name [HANDLE_MAX_SIZE];
    Column *columns;
    size_t col_capacity;
    size_t col_size;
    size_t table_capacity;
    size_t table_size;
} Table;

// track if we are loading an indexed table
extern Table* btree_indexed_table;

/**
 * db
 * Defines a database structure, which is composed of multiple tables.
 * - name: the name of the associated database.
 * - tables: the pointer to the array of tables contained in the db.
 * - tables_size: the size of the array holding table objects
 * - tables_capacity: the amount of pointers that can be held in the currently allocated memory slot
 **/

typedef struct Db {
    char name[HANDLE_MAX_SIZE];
    Table *tables;
    size_t tables_size;
    size_t tables_capacity;
} Db;

/**
 * Error codes used to indicate the outcome of an API call
 **/
typedef enum StatusCode {
  /* The operation completed successfully */
  OK,
  /* There was an error with the call. */
  ERROR,
} StatusCode;

// status declares an error code and associated message
typedef struct Status {
    StatusCode code;
    char* error_message;
} Status;

// Defines a comparator flag between two values.
typedef enum ComparatorType {
    NO_COMPARISON = 0,
    LESS_THAN = 1,
    GREATER_THAN = 2,
    EQUAL = 4,
    LESS_THAN_OR_EQUAL = 5,
    GREATER_THAN_OR_EQUAL = 6
} ComparatorType;

/*
 * Declares the type of a result column, 
 which includes the number of tuples in the result, the data type of the result, and a pointer to the result data
 */
typedef struct Result {
    size_t num_tuples;
    int data_type;
    void *payload;
    int bitvector_ints;
    bool is_posn_vector;
} Result;

/*
 * an enum which allows us to differentiate between columns and results
 */
typedef enum GeneralizedColumnType {
    RESULT,
    COLUMN
} GeneralizedColumnType;
/*
 * a union type holding either a column or a result struct
 */
typedef union GeneralizedColumnPointer {
    Result* result;
    Column* column;
} GeneralizedColumnPointer;

/*
 * unifying type holding either a column or a result
 */
typedef struct GeneralizedColumn {
    GeneralizedColumnType column_type;
    GeneralizedColumnPointer column_pointer;
} GeneralizedColumn;

/*
 * used to refer to a column in our client context
 */

typedef struct GeneralizedColumnHandle {
    char name[HANDLE_MAX_SIZE];
    GeneralizedColumn generalized_column;
} GeneralizedColumnHandle;

/*
 * holds the information necessary to refer to generalized columns (results or columns)
 */
typedef struct ClientContext {
    GeneralizedColumnHandle* chandle_table;
    int chandles_in_use;
    int chandle_slots;
} ClientContext;

/**
 * comparator
 * A comparator defines a comparison operation over a column. 
 **/
typedef struct Comparator {
    long int p_low; // used in equality and ranges.
    long int p_high; // used in range compares. 
    GeneralizedColumn gen_col;
    bool has_posn_vector;
    GeneralizedColumn posn_vector;
    ComparatorType type1;
    ComparatorType type2;
    //char* handle;
    char handle[HANDLE_MAX_SIZE];
} Comparator;

/*
 * tells the databaase what type of operator this is
 */
typedef enum OperatorType {
    CREATE_DB,
    CREATE_TABLE,
    CREATE_COLUMN,
    CREATE_INDEX,
    INSERT,
    UPDATE,
    DELETE,
    OPEN,
    SELECT,
    FETCH,
    JOIN,
    AVERAGE,
    SUM,
    MIN,
    MAX,
    ADD,
    SUB,
    SHUTDOWN,
    SHARED_QUERY_LOGGED,
    SHARED_SCAN,
    PRINT,
} OperatorType;

/*
 * necessary fields for creating a db
 */
typedef struct CreateDbOperator {
    char name[HANDLE_MAX_SIZE];
} CreateDbOperator;
/*
 * necessary fields for creating a table
 */
typedef struct CreateTableOperator {
    char db_name[HANDLE_MAX_SIZE];
    char name[HANDLE_MAX_SIZE];
    int col_count;
} CreateTableOperator;
/*
 * necessary fields for creating a column
 */
typedef struct CreateColumnOperator {
    char db_name[HANDLE_MAX_SIZE];
    char table_name[HANDLE_MAX_SIZE];
    char name[HANDLE_MAX_SIZE];
} CreateColumnOperator;
/*
 * necessary fields for creating an index
 */
typedef struct CreateIndexOperator {
    Column* column;
    IndexType index_type;
    bool clustered;
} CreateIndexOperator;
/*
 * necessary fields for insertion
 */
typedef struct InsertOperator {
    Table* table;
    int* values;
} InsertOperator;
/*
 * necessary fields for updates
 */
typedef struct UpdateOperator {
    Table* table;
    int column_idx; // this is the index of the column in the table
    Result* row_to_update;
    int new_value;
} UpdateOperator;
/*
 * necessary fields for deletes
 */
typedef struct DeleteOperator {
    Table* table;
    Result* row_to_delete;
} DeleteOperator;
/*
 * necessary fields for selection
 */
typedef struct SelectOperator {
    Comparator* compare_info;
    int num_results;
    int num_posn_results;
} SelectOperator;
/*
 * necessary fields for fetching
 */
typedef struct FetchOperator {
    Table* table;
    Result* ids_result;
    Column* column;
    char handle[HANDLE_MAX_SIZE];
} FetchOperator;
/*
 * necessary fields for joining
 */
typedef struct JoinOperator {
    int join_type; // NESTED_LOOP_JOIN or HASH_JOIN
    Result* pos1_result;
    Result* val1_result;
    Result* pos2_result;
    Result* val2_result;
    char left_handle[HANDLE_MAX_SIZE];
    char right_handle[HANDLE_MAX_SIZE];
} JoinOperator;
/*
 * necessary fields for averaging
 */
typedef struct AverageOperator {
    GeneralizedColumn generalized_column;
    int num_results;
    char handle[HANDLE_MAX_SIZE];
} AverageOperator;
/*
 * necessary fields for summing
 */
typedef struct SumOperator {
    GeneralizedColumn generalized_column;
    int num_results;
    char handle[HANDLE_MAX_SIZE];
} SumOperator;
/*
 * necessary fields for min
 */
typedef struct MinOperator {
    GeneralizedColumn generalized_column;
    int num_results;
    char handle[HANDLE_MAX_SIZE];
} MinOperator;
/*
 * necessary fields for max
 */
typedef struct MaxOperator {
    GeneralizedColumn generalized_column;
    int num_results;
    char handle[HANDLE_MAX_SIZE];
} MaxOperator;
/*
 * necessary fields for adding columns
 */
typedef struct AddOperator {
    GeneralizedColumn generalized_column1;
    int num_results1;
    GeneralizedColumn generalized_column2;
    int num_results2;
    char handle[HANDLE_MAX_SIZE];
} AddOperator;
/*
 * necessary fields for printing
 */
typedef struct PrintOperator {
    Result** results;
    int num_results;
} PrintOperator;
/*
 * necessary fields for open
 */
typedef struct OpenOperator {
    char* db_name;
} OpenOperator;

/*
 * union type holding the fields of any operator
 */
typedef union OperatorFields {
    CreateDbOperator create_db_operator;
    CreateTableOperator create_table_operator;
    CreateColumnOperator create_column_operator;
    CreateIndexOperator create_index_operator;
    InsertOperator insert_operator;
    UpdateOperator update_operator;
    DeleteOperator delete_operator;
    OpenOperator open_operator;
    SelectOperator select_operator;
    FetchOperator fetch_operator;
    JoinOperator join_operator;
    AverageOperator average_operator;
    SumOperator sum_operator;
    MinOperator min_operator;
    MaxOperator max_operator;
    AddOperator add_operator;
    PrintOperator print_operator;
} OperatorFields;
/*
 * DbOperator holds the following fields:
 * type: the type of operator to perform (i.e. insert, select, ...)
 * operator fields: the fields of the operator in question
 * client_fd: the file descriptor of the client that this operator will return to
 * context: the context of the operator in question. This context holds the local results of the client in question.
 */
typedef struct DbOperator {
    OperatorType type;
    OperatorFields operator_fields;
    int client_fd;
    ClientContext* context;
} DbOperator;

/*
 * SharedScanDbOperators holds the necessary information to batch queries together
 * num_dbos - the number of DbOperators we have accumulated for a shared scan
 * dbos - array of DbOperator pointers that need to be executed
 */
typedef struct SharedScanDbOperators {
    int num_dbos;
    int dbo_slots;
    DbOperator** dbos;
    Column* col;
    int num_entries;
} SharedScanDbOperators;

extern Db *current_db;
extern bool currently_batching_query;
extern SharedScanDbOperators shared_scan_operators;

Status db_startup();

/**
 * sync_db(db)
 * Saves the current status of the database to disk.
 *
 * db       : the database to sync.
 * returns  : the status of the operation.
 **/
Status sync_db(Db* db);

Status load(const char* file_name);
Status db_load(const char* file_name);

Status shutdown_server();
Status shutdown_database(Db* db);

char** execute_db_operator(DbOperator* query);
void db_operator_free(DbOperator* query);


#endif /* CS165_H */

