#ifndef DB_HELPERS_H
#define DB_HELPERS_H

#include "cs165_api.h"
#include "btree.h"

/* 
 * this function returns the column by which a table is clustered if it exists
 * additionally, indicates the index of this colum in the table
 */
Column* find_clustered_column(Table* table, int* column_idx);

/* 
 * this function finds the correct position of a record in a sorted array of 
 * DataEntry and returns the index of that position in the array. uses binary search
 */
int find_data_entry_record_position(DataEntry* data, int num_records, int value);

/* 
 * this function finds the correct position of a record in a sorted array of 
 * ints and returns the index of that position in the array. uses binary search
 */
int find_int_record_position(int* data, int num_records, int value);

/* 
 * this function shifts all the data in an array upwards shift_num positions
 * from a starting index. it moves from the end of the data downward
 */
void shift_int_data_up(int* data, int data_sz, int index, int shift_num);

/* 
 * this function shifts all the data in an array downwards shift_num positions
 * from a starting index. it moves from the start of the data upward
 */
void shift_int_data_down(int* data, int data_sz, int index, int shift_num);

/* 
 * this function shifts all the data in an array upwards shift_num positions
 * from a starting index. it moves from the end of the data downward
 */
void shift_data_up(DataEntry* data, int data_sz, int index, int shift_num);

/* 
 * this function shifts all the data in an array downwards shift_num positions
 * from a starting index. it moves from the start of the data upward
 */
void shift_data_down(DataEntry* data, int data_sz, int index, int shift_num);

/* 
 * this function shifts all the data in a table upwards from a starting index
 */
void shift_table_data_up(Table* table, unsigned int index);

/* 
 * this function shifts all the data in a table downwards
 * from a starting index. it moves from the start of the data upward.
 * returns the complete row of data that gets overwritten by shifting down
 */
int* shift_table_data_down(Table* table, unsigned int index);

/* 
 * this function doubles the size of an array of ints and returns that new
 * array. Note: frees the old array, also update the old_sz to be the new_sz
 */
int* resize_data(int* old_data, int* old_sz);

/* 
 * this function created a 1s mask from the left
 * ex: num_ones = 4
 * 00000000000000000000000001111
 */
int ones_mask_from_left(int num_ones);

/* 
 * this function created a 1s mask from the right
 * ex: num_ones = 4
 * 11110000000000000000000000000
 */
int ones_mask_from_right(int num_ones);

/* 
 * this function shifts all the pos values in a DataEntry up by one, useful
 * after we insert a value into an unclustered btree in a table that also
 * has a sorted, clustered index. not not to increment the value just inserted
 */
void shift_pos_values_up_one(BTree* btree, int relevant_pos, int value);

/* 
 * this function takes an array of DataEntrys and a position integer. it
 * iterates through the DataEntrys, decrementing the pos of all DataEntrys
 * that have a pos larger than row_pos. it also removes the matching row_pos
 * and shifts the remaining data downward.
 */
void delete_and_shift_down_pos(DataEntry* data, int data_sz, int row_pos);

/* 
 * this function returns a bool indicating whether or not a table has a
 * clustered index
 */
bool has_clustered_index(Table* table);

#endif /* DB_HELPERS_H */
