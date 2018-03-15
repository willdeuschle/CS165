#ifndef DB_READS_INDEXED_H
#define DB_READS_INDEXED_H

#include "cs165_api.h"

/* 
 * this function finds the index of the first value greater than or equal to
 * low_value in a sorted array of data
 */
int sorted_column_index_gte(int* data, int num_records, int low_value);

/* 
 * this function finds the index of the last less than high_value
 * in a sorted array of data
 */
int sorted_column_index_lt(int* data, int num_records, int high_value);

/* 
 * this function creates a position vector of qualifying indices from a sorted
 * index of DataEntry's
 */
int* sorted_data_entry_select_range(DataEntry* data, int low_value, int high_value, int num_entries, int* num_records);

/* 
 * this function takes a start and end value, and marks the values in between
 * them (inclusive) as present (1) in the bit vector result
 */
void mark_bv_range(int* result, int start_idx, int end_idx);

/* 
 * this function marks a bitvector using the qualifying positions from a
 * position vector
 */
void mark_bv_from_pos_vec(int* result, int* pos_vec, int num_results);

/* 
 * this function returns a bool indicating whether or not a position is marked
 * in a bitvector
 */
bool check_bv_position(int* bv, int pos);

/* 
 * this function takes a bitvector and returns a position vector representing it
 */
int* pos_vector_from_bv(int* bv, int num_bv_ints);

#endif /* DB_READS_INDEXED_H */
