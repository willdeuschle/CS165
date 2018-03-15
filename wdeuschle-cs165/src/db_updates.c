#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "cs165_api.h"
#include "utils.h"
#include "db_helpers.h"
#include "btree.h"
#include "db_reads_indexed.h"


/* 
 * this function doubles the capacity of a table and copies over the old
 * values
 */
void resize_table(Table* table) {
    log_info("resizing table: %s\n", table->name);
    // number of columns in this table
    int new_capacity = table->table_capacity * 2;
    // for each column, create a new column and copy over the data
    for (unsigned int i = 0; i < table->col_size; ++i) {
        Column* current_col = table->columns + i;
        int* new_data = malloc(new_capacity * sizeof(int));
        int* old_data = current_col->data;
        // copy all that data to our new column
        memcpy(new_data, old_data, (table->table_capacity) * sizeof(int));
        current_col->data = new_data;
        free(old_data);

        // check for an unclustered SORTED index to resize
        if (!current_col->clustered && current_col->index_type == SORTED) {
            DataEntry* old_clustered_index = (DataEntry*) current_col->index;
            DataEntry* new_clustered_index = malloc(new_capacity * sizeof(DataEntry));
            // copy all that data to our new index
            memcpy(new_clustered_index, old_clustered_index, (table->table_capacity) * sizeof(DataEntry));
            current_col->index = new_clustered_index;
            free(old_clustered_index);
        }
    }
    table->table_capacity = new_capacity;
}

/* 
 * this function inserts a value and position into a sorted column index
 */
void insert_into_sorted_index(DataEntry* data, int num_records, int value, int pos) {
    int pos_idx = find_data_entry_record_position(data, num_records, value);
    // shift data upward based on this index
    shift_data_up(data, num_records, pos_idx, 1);
    // insert new_value
    DataEntry new_data_entry = { value, pos };
    data[pos_idx] = new_data_entry;
    return;
}

/* 
 * this function inserts a row into a table
 */
void insert_row(Table* table, int* input_values) {
    // resize table if necessary
    if (table->table_size == table->table_capacity) {
        resize_table(table);
    }
    // number of columns in this table
    int num_values = table->col_capacity;
    // number of rows in the table
    int current_table_size = (int) table->table_size;
    // which row in the table we need to update
    int row_to_update = current_table_size;

    // find clustered column if it exists
    int column_idx;
    Column* clustered_column = find_clustered_column(table, &column_idx);
    if (clustered_column != NULL) {
        log_info("clustered column found\n");
        // need to maintain the sort on this clustered column
        // if it's a sorted cluster
        if (clustered_column->index_type == SORTED) {
            log_info("sorted clustered\n");
            int column_value = input_values[column_idx];
            row_to_update = find_int_record_position(clustered_column->data, table->table_size, column_value);
            // shift data upward based on this index
            shift_table_data_up(table, row_to_update);
        } else {
            log_info("btree clustered\n");
            int column_value = input_values[column_idx];
            // find the pos for this value
            row_to_update = find_int_record_position(clustered_column->data, table->table_size, column_value);
            // don't update all the positions in the btree if we're loading a 
            // table, do that at the end
            btree_insert((BTree*) clustered_column->index, column_value, row_to_update, !btree_indexed_load);
            // shift data upward based on this index
            shift_table_data_up(table, row_to_update);
        }
    }

    // update each column
    for (int i = 0; i < num_values; i++) {
        // access each value
        int val = input_values[i];
        // insert the value into the appropriate column
        table->columns[i].data[row_to_update] = val;
    }

    // update unclustered indexes
    // iterate through each column and check for unclustered indexes
    for (size_t i = 0; i < table->col_size; ++i) {
        Column current_column = table->columns[i];
        if (current_column.index_type == NO_INDEX || current_column.clustered) {
            // no index or clustered index, continue
            continue;
        } else if (current_column.index_type == SORTED) {
            // insert into an unclustered sorted column
            // take value from position of this column
            insert_into_sorted_index((DataEntry*) current_column.index, table->table_size, input_values[i], row_to_update);
        } else if (current_column.index_type == BTREE) {
            // determine if another column is clustered here so we know if
            // we need to update all the other indexes
            bool update_on_clustered_index = has_clustered_index(table);
            // if there is an indexed load in progress, don't update the positions each time (too slow),
            // we'll do it in one fell swoop at the end
            update_on_clustered_index = update_on_clustered_index && !btree_indexed_load;
            // insert into unclustered btree
            // take value from position of this column
            btree_insert((BTree*) current_column.index, input_values[i], row_to_update, update_on_clustered_index);
        } else {
            log_err("INDEX TYPE NOT IMPLEMENTED\n");
            abort();
        }
    }

    // update the table size
    table->table_size++;


    return;
}

/* 
 * this function deletes a row from a table and returns the deleted row  to
 * the caller
 */
int* delete_row(Table* table, Result* row_to_delete) {
    // row to return - going to populate this before we actually overwrite
    // the data
    int* deleted_row;
    // position of the row to delete
    int row_pos;
    // determine if this is a bit or position vector
    if (row_to_delete->is_posn_vector) {
        // only supporting a single item delete
        assert(row_to_delete->num_tuples = 1);
        row_pos = ((int*) row_to_delete->payload)[0];
    } else {
        // only supporting a single item delete
        assert(row_to_delete->num_tuples = 1);
        assert(row_to_delete->bitvector_ints > 0);
        // turn it into a position vector
        int* pos_vec = pos_vector_from_bv((int*) row_to_delete->payload, row_to_delete->bitvector_ints);
        row_pos = pos_vec[0];
        // free the pos vec
        free(pos_vec);
    }
    // now we have the row position
    // first need to handle the base data / clustered indexes
    // determine if we are clustered on anything
    IndexType index_type = NO_INDEX;
    Column* index_column = NULL;
    for (unsigned int i = 0; i < table->col_size; ++i) {
        Column* current_col = table->columns + i;
        // needs to be a clustered column
        if (current_col->index_type != NO_INDEX && current_col->clustered) {
            index_type = current_col->index_type;
            index_column = current_col;
            assert(index_type == SORTED || index_type == BTREE);
            break;
        }
    }
    // sorted clustered index or no index
    if (index_type == SORTED || index_type == NO_INDEX) {
        // just need to shift table data down and decrement the number of entries
        // shift every column down
        deleted_row = shift_table_data_down(table, row_pos);
    } else {
        // btree clustered index
        assert(index_type == BTREE);
        // need to update our btree, then update the rest of the data based on that
        int entry_location = -1;
        Node* node_to_update = btree_find_nth_entry((BTree*) index_column->index, row_pos, &entry_location);
        // should have a node and entry location should have changed
        assert(node_to_update && node_to_update->is_leaf && entry_location != -1);
        // now need to shift all the data down in this node entry, decrement
        // the number of entries, update the parent signposts, decrement the
        // pos of all the entries above this one
        btree_shift_update_decrement(node_to_update, entry_location);
        // now need to update all the base data
        deleted_row = shift_table_data_down(table, row_pos);
    }

    // now need to handle unclustered indexes
    // for btree and sorted: for every DataEntry, need to decrement the pos
    // if it's > the pos we're deleting. also need to remove the relevant
    // data entry and shift data downwards as necessary, update the number
    // of elements in these extra data structures
    // check each column to see if it has an unclustered index on it
    for (unsigned int i = 0; i < table->col_size; ++i) {
        Column* current_col = table->columns + i;
        if (current_col->clustered == false) {
            if (current_col->index_type == SORTED) {
                // remove this entry for unclustered sorted index
                delete_and_shift_down_pos((DataEntry*) current_col->index, table->table_size, row_pos);
            } else if (current_col->index_type == BTREE) {
                // remove this entry for unclustered btree index
                btree_delete_and_shift_down_pos((BTree*) current_col->index, row_pos);
            }
        }
    }

    // now decrement the number of entries in the table
    --table->table_size;
    // return the deleted row
    return deleted_row;
}

/* 
 * this function handles a query to insert a row into the database, and 
 * returns the result message
 */
void db_insert(DbOperator* query, message* send_message) {
    Table* table = query->operator_fields.insert_operator.table;
    // resize table if necessary
    if (table->table_size == table->table_capacity) {
        resize_table(table);
    }
    // insert the row
    insert_row(query->operator_fields.insert_operator.table, query->operator_fields.insert_operator.values);

    // return successful result
    const char* result_message = "insert successful";
    char* result = malloc(strlen(result_message) + 1);
    strcpy(result, result_message);
    send_message->payload = result;
    send_message->status = OK_DONE;
    return;
}

/* 
 * this function handles a query to update a row into the database, and 
 * returns the result message
 */
void db_update(DbOperator* query, message* send_message) {
    Table* table = query->operator_fields.update_operator.table;
    int column_idx = query->operator_fields.update_operator.column_idx;
    Result* row_to_update = query->operator_fields.update_operator.row_to_update;
    int new_value = query->operator_fields.update_operator.new_value;


    // first delete the row
    int* deleted_row = delete_row(table, row_to_update);
    // update the value in the deleted row that needs to be updated
    deleted_row[column_idx] = new_value;
    insert_row(query->operator_fields.insert_operator.table, deleted_row);
    // free the deleted row
    free(deleted_row);

    // return successful result
    const char* result_message = "update successful";
    char* result = malloc(strlen(result_message) + 1);
    strcpy(result, result_message);
    send_message->payload = result;
    send_message->status = OK_DONE;
    return;
}

/* 
 * this function handles a query to delete a row into the database, and 
 * returns the result message
 */
void db_delete(DbOperator* query, message* send_message) {
    Table* table = query->operator_fields.delete_operator.table;
    Result* row_to_delete = query->operator_fields.delete_operator.row_to_delete;

    // delete the row
    int* deleted_row = delete_row(table, row_to_delete);
    // free the deleted row
    free(deleted_row);

    // return successful result
    const char* result_message = "delete successful";
    char* result = malloc(strlen(result_message) + 1);
    strcpy(result, result_message);
    send_message->payload = result;
    send_message->status = OK_DONE;
    return;
}
        
/* 
 * this function updates btree indexes after a table load
 */
void btree_update_for_indexed_table(Table* table) {
    // find btree indexes and update them based on whether they are clustered
    // or unclustered
    for (unsigned int i = 0; i < table->col_size; ++i) {
        Column* current_col = table->columns + i;
        if (current_col->index_type == BTREE) {
            BTree* btree = (BTree*) current_col->index;
            if (current_col->clustered) {
                printf("UPDATING CLUSTERED BTREE AFTER LOAD\n");
                // this tree needs updating
                assert(btree != NULL);
                // so this btree needs updating. iterate through every data item in
                // the btree, adding its pos as we go.
                // first get the leaf node
                Node* current_node = btree->root;
                while (!current_node->is_leaf) {
                    current_node = current_node->payload.signposts[0].node_pointer;
                }
                // now we have the leftmost leaf node
                assert(current_node->is_leaf);
                int global_idx_count = 0;
                while (current_node) {
                    for (int i = 0; i < current_node->num_entries; ++i) {
                        // the global_idx_count is this post, also increment it for
                        // the next iteration
                        current_node->payload.data[i].pos = global_idx_count++;
                    }
                    // get the next node
                    current_node = current_node->next;
                }
            } else {
                printf("UPDATING UNCLUSTERED BTREE AFTER LOAD\n");
                // this tree needs updating
                assert(current_col != NULL);
                assert(btree != NULL);
                // so this btree needs updating. iterate through every item in the
                // column, updating the pos of that element in the btree as we go
                for (unsigned int i = 0; i < table->table_size; ++i) {
                    int current_val = current_col->data[i]; // get this value
                    // get the node and index with this value from the btree
                    int node_idx = -1;
                    // get the node and index
                    Node* this_node = btree_gte_probe(btree, current_val, &node_idx);
                    assert(node_idx != -1);
                    // needs to be the same
                    assert(this_node->payload.data[node_idx].value == current_val);
                    // update the position associated with this value
                    this_node->payload.data[node_idx].pos = i;
                }
            }
        }
    }
    return;
}
