#include <assert.h>
#include <string.h>
#include "cs165_api.h"
#include "db_helpers.h"
#include "utils.h"

/* 
 * this function returns the column by which a table is clustered if it exists
 * additionally, indicates the index of this colum in the table
 */
Column* find_clustered_column(Table* table, int* column_idx) {
    *column_idx = -1;
    Column* clustered_column = NULL;

    for (unsigned int i = 0; i < table->col_size; ++i) {
        Column* current_col = &table->columns[i];
        if (current_col->clustered) {
            clustered_column = current_col;
            *column_idx = i;
            break;
        }
    }

    return clustered_column;
}

/* 
 * this function finds the correct position of a record in a sorted array
 * and returns the index of that position in the array. uses binary search
 */
int find_data_entry_record_position(DataEntry* data, int num_records, int value) {
    // binary search
    int start = 0;
    int end = num_records - 1;
    int middle_idx = -1;

    while (start <= end) {
        middle_idx = (start + end) / 2;
        int middle_val = data[middle_idx].value;

        if (value == middle_val) {
            // found the appropriate index
            while (middle_idx < num_records - 1 && data[middle_idx + 1].value == value) {
                ++middle_idx;
            }
            if (middle_idx < num_records - 1) {
                assert(data[middle_idx + 1].value != value);
            }
            return middle_idx;
        } else if (value > middle_val) {
            // recurse right
            start = middle_idx + 1;
        } else {
            end = middle_idx - 1;
        }
    }
    
    // want to move everything up one, so we return start now
    return start;
}

/* 
 * this function finds the correct position of a record in a sorted array
 * and returns the index of that position in the array. uses binary search
 */
int find_int_record_position(int* data, int num_records, int value) {
    // binary search
    int start = 0;
    int end = num_records - 1;
    int middle_idx = -1;

    while (start <= end) {
        middle_idx = (start + end) / 2;
        int middle_val = data[middle_idx];

        if (value == middle_val) {
            // found the appropriate index
            return middle_idx;
        } else if (value > middle_val) {
            // recurse right
            start = middle_idx + 1;
        } else {
            end = middle_idx - 1;
        }
    }
    
    // want to move everything up one, so we return start now
    return start;
}

/* 
 * this function shifts all the data in an array upwards shift_num positions
 * from a starting index. it moves from the end of the data downward
 */
void shift_int_data_up(int* data, int data_sz, int index, int shift_num) {
    // we aren't moving index, but we are moving the value right above it
    // make sure that value fits within the bounds of the data array
    assert(index >= 0);
    // shift each item
    int new_end_idx = data_sz - 1 + shift_num;
    int new_beg_idx = index + shift_num;
    for (int i = new_end_idx; i >= new_beg_idx; --i) {
        data[i] = data[i - shift_num];
    }
    return;
}

/* 
 * this function shifts all the data in an array downwards shift_num positions
 * from a starting index. it moves from the start of the data upward
 */
void shift_int_data_down(int* data, int data_sz, int index, int shift_num) {
    // we move everything from index -> index - shift_num
    // shouldn't move below 0
    assert(index - shift_num >= 0);
    // shift each data item
    for (int i = index; i < data_sz; ++i) {
        data[i - shift_num] = data[i];
    }
    return;
}

/* 
 * this function shifts all the data in an array upwards shift_num positions
 * from a starting index. it moves from the end of the data downward
 */
void shift_data_up(DataEntry* data, int data_sz, int index, int shift_num) {
    // we aren't moving index, but we are moving the value right above it
    // make sure that value fits within the bounds of the data array
    assert(index >= 0);
    // shift each data item
    int new_end_idx = data_sz - 1 + shift_num;
    int new_beg_idx = index + shift_num;
    for (int i = new_end_idx; i >= new_beg_idx; --i) {
        data[i] = data[i - shift_num];
    }
    return;
}

/* 
 * this function shifts all the data in an array downwards shift_num positions
 * from a starting index. it moves from the start of the data upward
 */
void shift_data_down(DataEntry* data, int data_sz, int index, int shift_num) {
    // we move everything from index -> index - shift_num
    // shouldn't move below 0
    assert(index - shift_num >= 0);
    // shift each data item
    for (int i = index; i < data_sz; ++i) {
        data[i - shift_num] = data[i];
    }
    return;
}

/* 
 * this function shifts all the data in a table upwards from a starting index
 * starts at the end of the data and moves downward
 */
void shift_table_data_up(Table* table, unsigned int index) {
    log_info("shifting table data up\n");
    // for each column
    for (unsigned int i = 0; i < table->col_size; ++i) {
        Column* current_col = &table->columns[i];
        // shift each data item
        for (unsigned int j = table->table_size; j > index; --j) {
            current_col->data[j] = current_col->data[j - 1];
        }
    }
    return;
}

/* 
 * this function shifts all the data in a table downwards
 * from a starting index. it moves from the start of the data upward.
 * returns the complete row of data that gets overwritten by shifting down
 */
int* shift_table_data_down(Table* table, unsigned int index) {
    // deleted row, return this
    int* deleted_row = malloc(sizeof(int) * table->col_size);
    log_info("shifting table data down\n");
    // for each column
    for (unsigned int i = 0; i < table->col_size; ++i) {
        Column* current_col = table->columns + i;
        // get the value in this column before overwriting it
        deleted_row[i] = current_col->data[index];
        shift_int_data_down(current_col->data, table->table_size, index + 1, 1);
    }
    return deleted_row;
}

/* 
 * this function doubles the size of an array of ints and returns that new
 * array. Note: frees the old array, also update the old_sz to be the new_sz
 */
int* resize_data(int* old_data, int* old_sz) {
    int new_sz = *old_sz * 2;
    int* new_data = malloc(new_sz * sizeof(int));
    // copy all the old data to the new array
    memcpy(new_data, old_data, *old_sz * sizeof(int));
    free(old_data);
    // update the size
    *old_sz = new_sz;
    return new_data;
}

/* 
 * this function created a 1s mask from the left
 * ex: num_ones = 4
 * 00000000000000000000000001111
 */
int ones_mask_from_left(int num_ones) {
    if (num_ones == 0) {
        return 0;
    }
    int mask = (1 << num_ones) - 1;
    return mask;
}

/* 
 * this function created a 1s mask from the right
 * ex: num_ones = 4
 * 11110000000000000000000000000
 */
int ones_mask_from_right(int num_ones) {
    int shift_num = BITS_PER_INT - num_ones;
    int flipped_mask = ones_mask_from_left(shift_num);
    return -1 ^ flipped_mask;
}

/* 
 * this function shifts all the pos values in a DataEntry up by one, useful
 * after we insert a value into an unclustered btree in a table that also
 * has a sorted, clustered index
 */
void shift_pos_values_up_one(BTree* btree, int new_pos, int value) {
    Node* current_node = btree->root;
    // get the far left leaf node
    for (int i = 1; i < btree->height; ++i) {
        current_node = current_node->payload.signposts[0].node_pointer;
    }
    assert(current_node->is_leaf); // this the far left leaf node
    int current_idx = 0; // start at the far left in the data
    while (current_node) {
        if (current_idx >= current_node->num_entries) {
            current_node = current_node->next;
            current_idx = 0;
            continue;
        } else {
            // increment the pos if it's >= the one we inserted, also make sure we don't decrement the entry we just added
            if (current_node->payload.data[current_idx].pos >= new_pos && current_node->payload.data[current_idx].value != value) {
                ++current_node->payload.data[current_idx].pos;
            }
            ++current_idx;
        }
    }
    return;
}

/* 
 * this function takes an array of DataEntrys and a position integer. it
 * iterates through the DataEntrys, decrementing the pos of all DataEntrys
 * that have a pos larger than row_pos. it also removes the matching row_pos
 * and shifts the remaining data downward.
 */
void delete_and_shift_down_pos(DataEntry* data, int data_sz, int row_pos) {
    for (int i = 0; i < data_sz; ++i) {
        if (data[i].pos == row_pos) {
            // remove this data entry by shifting everything down on top of it
            shift_data_down(data, data_sz, i, 1);
        } else if (data[i].pos > row_pos) {
            // decrement this pos
            --data[i].pos;
        }
    }
    return;
}

/* 
 * this function returns a bool indicating whether or not a table has a
 * clustered index
 */
bool has_clustered_index(Table* table) {
    for (unsigned int i = 0; i < table->col_size; ++i) {
        if (table->columns[i].clustered) {
            return true;
        }
    }
    return false;
}
