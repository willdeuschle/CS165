#ifndef DB_JOIN_H
#define DB_JOIN_H

#include "cs165_api.h"

typedef struct PartitionSet {
    int* left_partition_values; // the values from the left column
    int* left_partition_positions; // the positions of the values in the left column
    int left_partition_size; // the number of slots in the left_partition_values/positions array
    int num_entries_left_partition; // the number of entries currently in left_partition_values/positions
    // all the same meaning as for the data above
    int* right_partition_values;
    int* right_partition_positions;
    int right_partition_size;
    int num_entries_right_partition;
} PartitionSet;

/* 
 * this function handles execution of the join query by delegating to the
 * proper join function
 */
void db_join(DbOperator* query, message* send_message);

/* 
 * this function handles a join using a nested loop algorithm
 */
void db_join_nested_loop(DbOperator* query, message* send_message);

/* 
 * this function given a value and a number of partition buckets, returns the
 * proper bucket for that value
 */
int get_partition_bucket(int value, int num_buckets);

/* 
 * this function generates partitions from two columns so that we can
 * perform a more cache-conscious join
 */
PartitionSet* generate_partitions(
    int* left_value_vector, int* left_position_vector, int left_size,
    int* right_value_vector, int* right_position_vector, int right_size,
    int num_partitions);

/* 
 * this function frees an array of partition sets
 */
void free_partition_sets(PartitionSet* partition_set, int num_partitions);

/* 
 * this function inserts join data into the left and right results, 
 * keeping data in the join in the order of left column (which is arbitryary)
 * this requires shifting data in both columns
 */
void insert_to_result_and_maintain_order(int* left_result_column, int left_value, int* right_result_column, int right_value, int* num_results);

/* 
 * this function handles a join using a hash join algorithm
 */
void db_join_hash(DbOperator* query, message* send_message);

#endif
