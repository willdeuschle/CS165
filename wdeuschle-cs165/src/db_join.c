#include <string.h>
#include <assert.h>
#include "cs165_api.h"
#include "utils.h"
#include "db_join.h"
#include "db_reads_indexed.h"
#include "client_context.h"
#include "db_helpers.h"
#include "hash_table.h"

#define INTS_PER_PAGE ((int) (4096 / sizeof(int)))
#define STARTING_RESULT_CAPACITY 4096
// eventually figure out how many partitions actually works best, or even do it dynamically
#define NUM_PARTITIONS 64

/* 
 * this function handles execution of the join query by delegating to the
 * proper join function
 */
void db_join(DbOperator* query, message* send_message) {
    if (query->operator_fields.join_operator.join_type == NESTED_LOOP_JOIN) {
        db_join_nested_loop(query, send_message);
        return;
    } else if (query->operator_fields.join_operator.join_type == HASH_JOIN) {
        db_join_hash(query, send_message);
        return;
    } else {
        log_err("FAILURE, UNRECOGNIZED JOIN ALGORITHM\n");
        const char* result_message = "join failed, unrecognized join algorithm\n";
        char* results_str = malloc(strlen(result_message) + 1);
        strcpy(results_str, result_message);
        send_message->status = EXECUTION_ERROR;
        send_message->payload = results_str;
    }
    return;
}

/* 
 * this function handles a join using a nested loop algorithm
 */
void db_join_nested_loop(DbOperator* query, message* send_message) {
    log_info("db_join_nested_loop\n");
    // first create our two new results bitvectors
    // use the size of the input position vectors to create our results bitvectors
    // TODO: can you use bitvectors here? I don't think so in the end because
    // of duplicates
    /*int* left_bitvector_res = calloc(query->operator_fields.join_operator.pos1_result->bitvector_ints, sizeof(int));*/
    /*int* right_bitvector_res = calloc(query->operator_fields.join_operator.pos2_result->bitvector_ints, sizeof(int));*/
    int result_arr_size = STARTING_RESULT_CAPACITY;
    int* left_result_vector = NULL; // need to assign after joining
    int* right_result_vector = NULL; // need to assign after joining
    // need position vectors from the position bitvectors
    int* left_position_vector = pos_vector_from_bv((int*) query->operator_fields.join_operator.pos1_result->payload, query->operator_fields.join_operator.pos1_result->bitvector_ints);
    int* right_position_vector = pos_vector_from_bv((int*) query->operator_fields.join_operator.pos2_result->payload, query->operator_fields.join_operator.pos2_result->bitvector_ints);
    int num_results = 0;

    // the larger of the two comparison vectors is the one we want to iterate over
    // set up the correct result vector for each of them
    Result* to_iterate = NULL;
    int* to_iterate_pos_vec = NULL; // hold position information about current values
    int* to_iterate_res_vec = malloc(sizeof(int) * result_arr_size); // for results
    Result* hold_in_place = NULL;
    int* hold_in_place_pos_vec = NULL; // for results
    int* hold_in_place_res_vec = malloc(sizeof(int) * result_arr_size); // hold position information about current values

    // need to know the size of the left and right results to choose which
    // we iterate over
    int left_size = query->operator_fields.join_operator.val1_result->num_tuples;
    int right_size = query->operator_fields.join_operator.val2_result->num_tuples;

    if (left_size > right_size) {
        to_iterate = query->operator_fields.join_operator.val1_result;
        to_iterate_pos_vec = left_position_vector;
        hold_in_place = query->operator_fields.join_operator.val2_result;
        hold_in_place_pos_vec = right_position_vector;
    } else {
        to_iterate = query->operator_fields.join_operator.val2_result;
        to_iterate_pos_vec = right_position_vector;
        hold_in_place = query->operator_fields.join_operator.val1_result;
        hold_in_place_pos_vec = left_position_vector;
    }

    int* to_iterate_data = (int*) to_iterate->payload;
    int* hold_in_place_data = (int*) hold_in_place->payload;

    // determine how many pages of the larger one we need to read in
    int num_pages_of_larger = (to_iterate->num_tuples / INTS_PER_PAGE) + (to_iterate->num_tuples % INTS_PER_PAGE > 0);
    // now we iterator over the larger vector, making comparisons as we go
    for (int i = 0; i < num_pages_of_larger; ++i) {
        // get the next page of the larger array
        int* next_larger_page = to_iterate_data + (INTS_PER_PAGE * i);
        int num_to_iterate;
        if (i == num_pages_of_larger - 1) {
            num_to_iterate = to_iterate->num_tuples - (INTS_PER_PAGE * i);
        } else {
            num_to_iterate = INTS_PER_PAGE;
        }
        for (int j = 0; j < num_to_iterate; ++j) {
            // get the next value from the larger array
            int value_from_larger = next_larger_page[j];
            // iterate over all the values in the smaller array
            for (int k = 0; k < (int) hold_in_place->num_tuples; ++k) {
                if (value_from_larger == hold_in_place_data[k]) {
                    // add position to result vector of the larger and smaller array
                    int large_pos = to_iterate_pos_vec[(INTS_PER_PAGE * i) + j]; // access the correct position
                    int small_pos = hold_in_place_pos_vec[k];
                    to_iterate_res_vec[num_results] = large_pos;
                    // need to keep the order of the smaller, which could fall
                    // out of order otherwise
                    hold_in_place_res_vec[num_results++] = small_pos; // also increment num_results

                    // resize if necessary
                    if (num_results == result_arr_size) {
                        int old_size = result_arr_size;
                        to_iterate_res_vec = resize_data(to_iterate_res_vec, &old_size);
                        // note we pass result_arr_size here, it's because this
                        // function resizes that parameter
                        hold_in_place_res_vec = resize_data(hold_in_place_res_vec, &result_arr_size);
                    }

                    // from before, when using bitvectors. is this possible?
                    /*if (!check_bv_position(to_iterate_res_vec, large_pos_to_mark)) {*/
                        /*// larger*/
                        /*mark_bv_from_pos_vec(to_iterate_res_vec, &large_pos_to_mark, 1);*/
                        /*++num_results_larger;*/
                    /*}*/
                    /*if (!check_bv_position(hold_in_place_res_vec, small_pos_to_mark)) {*/
                        /*// smaller*/
                        /*mark_bv_from_pos_vec(hold_in_place_res_vec, &small_pos_to_mark, 1);*/
                        /*++num_results_smaller;*/
                    /*}*/
                }
            }
        }
    }

    // need to reassign these, they get lost when resizing
    if (left_size > right_size) {
        left_result_vector = to_iterate_res_vec;
        right_result_vector = hold_in_place_res_vec;
    } else {
        left_result_vector = hold_in_place_res_vec;
        right_result_vector = to_iterate_res_vec;
    }

    // copy results into client context
    // create the left result object
    Result* left_result_obj = malloc(sizeof(Result));
    // no tuples in this result object
    left_result_obj->num_tuples = num_results;
    left_result_obj->payload = left_result_vector;
    left_result_obj->data_type = INT;
    left_result_obj->bitvector_ints = -1; // no bitvector ints, result is not a bitvector
    left_result_obj->is_posn_vector = true; // need to specify, because usually we use bitvectors
    // wrap the result object appropriately
    GeneralizedColumnHandle left_generalized_result_handle;
    // add name to result wrapper
    strcpy(left_generalized_result_handle.name, query->operator_fields.join_operator.left_handle);
    // update type
    left_generalized_result_handle.generalized_column.column_type = RESULT;
    // add the result
    left_generalized_result_handle.generalized_column.column_pointer.result = left_result_obj;
    // add this value to the client context variable pool
    add_to_client_context(query->context, left_generalized_result_handle);

    // create the right result object
    Result* right_result_obj = malloc(sizeof(Result));
    // no tuples in this result object
    right_result_obj->num_tuples = num_results;
    right_result_obj->payload = right_result_vector;
    right_result_obj->data_type = INT;
    right_result_obj->bitvector_ints = -1; // no bitvector ints, result is not a bitvector
    right_result_obj->is_posn_vector = true; // need to specify, because usually we use bitvectors
    // wrap the result object appropriately
    GeneralizedColumnHandle right_generalized_result_handle;
    // add name to result wrapper
    strcpy(right_generalized_result_handle.name, query->operator_fields.join_operator.right_handle);
    // update type
    right_generalized_result_handle.generalized_column.column_type = RESULT;
    // add the result
    right_generalized_result_handle.generalized_column.column_pointer.result = right_result_obj;
    // add this value to the client context variable pool
    add_to_client_context(query->context, right_generalized_result_handle);

    // free things
    // position vectors
    free(to_iterate_pos_vec);
    free(hold_in_place_pos_vec);

    const char* result_message = "nested join successful";
    char* result_message_ptr = malloc(strlen(result_message) + 1);
    strcpy(result_message_ptr, result_message);
    send_message->payload = result_message_ptr;
    send_message->status = OK_DONE;

    return;
}

/* 
 * this function given a value and a number of partition buckets, returns the
 * proper bucket for that value
 */
int get_partition_bucket(int value, int num_buckets) {
    // the bucket we select is going to be based on the final x digits of the value
    // first figure out how many digits we're going to use based on the number of buckets
    int start_val = 1;
    int num_digits = 0;
    while (start_val < num_buckets) {
        start_val *= 2;
        ++num_digits;
    }
    // should only choose power of 2 num_buckets for simplicity
    assert(start_val = num_buckets);
    // now create our mask, only use the first num_digits digits
    // this creates a mask like 0000011111, where there are num_buckets ones
    int mask = (1 << num_digits) - 1;
    int bucket_number = value & mask;
    return bucket_number;
}

/* 
 * this function generates partitions from two columns so that we can
 * perform a more cache-conscious join
 */
PartitionSet* generate_partitions(
    int* left_value_vector, int* left_position_vector, int left_size,
    int* right_value_vector, int* right_position_vector, int right_size,
    int num_partitions) {
    // object to return
    PartitionSet* partition_set = malloc(sizeof(PartitionSet) * num_partitions);
    // initialize each PartitionSet to have 0 left and right entries
    for (int i = 0; i < num_partitions; ++i) {
        PartitionSet* current_partition = partition_set + i;
        current_partition->num_entries_left_partition = 0; // currently no entries
        current_partition->left_partition_size = STARTING_RESULT_CAPACITY; // size of the left arrays
        current_partition->left_partition_values = malloc(sizeof(int) * current_partition->left_partition_size); // allocate space for the values
        current_partition->left_partition_positions = malloc(sizeof(int) * current_partition->left_partition_size); // allocate space for the positions
        // all the same for the right
        current_partition->num_entries_right_partition = 0;
        current_partition->right_partition_size = STARTING_RESULT_CAPACITY;
        current_partition->right_partition_values = malloc(sizeof(int) * current_partition->right_partition_size);
        current_partition->right_partition_positions = malloc(sizeof(int) * current_partition->right_partition_size);
    }
    // now iterate through the left input column, partitioning as we go
    for (int i = 0; i < left_size; ++i) {
        int current_val = left_value_vector[i];
        int current_pos = left_position_vector[i];
        // get the bucket for this value
        int bucket = get_partition_bucket(current_val, NUM_PARTITIONS);
        assert(bucket < NUM_PARTITIONS);
        // get that partition
        PartitionSet* this_partition = partition_set + bucket;
        // now add this value and position to the left portion of that partition
        this_partition->left_partition_values[this_partition->num_entries_left_partition] = current_val;
        // increment the number of entries on this one
        this_partition->left_partition_positions[this_partition->num_entries_left_partition++] = current_pos;
        // resize the left partition if necessary
        if (this_partition->num_entries_left_partition == this_partition->left_partition_size) {
            int old_size = this_partition->left_partition_size;
            this_partition->left_partition_values = resize_data(this_partition->left_partition_values, &old_size);
            // note we pass left_partition_size here, it's because this
            // function resizes that parameter, and we want that to happen, but
            // not until we've had a chance to resize both the values and positions vectors
            this_partition->left_partition_positions = resize_data(this_partition->left_partition_positions, &this_partition->left_partition_size);
        }
    }

    // now iterate through the right input column, partitioning as we go
    for (int i = 0; i < right_size; ++i) {
        int current_val = right_value_vector[i];
        int current_pos = right_position_vector[i];
        // get the bucket for this value
        int bucket = get_partition_bucket(current_val, NUM_PARTITIONS);
        assert(bucket < NUM_PARTITIONS);
        // get that partition
        PartitionSet* this_partition = partition_set + bucket;
        // now add this value and position to the right portion of that partition
        this_partition->right_partition_values[this_partition->num_entries_right_partition] = current_val;
        // increment the number of entries on this one
        this_partition->right_partition_positions[this_partition->num_entries_right_partition++] = current_pos;
        // resize the right partition if necessary
        if (this_partition->num_entries_right_partition == this_partition->right_partition_size) {
            int old_size = this_partition->right_partition_size;
            this_partition->right_partition_values = resize_data(this_partition->right_partition_values, &old_size);
            // note we pass right_partition_size here, it's because this
            // function resizes that parameter, and we want that to happen, but
            // not until we've had a chance to resize both the values and positions vectors
            this_partition->right_partition_positions = resize_data(this_partition->right_partition_positions, &this_partition->right_partition_size);
        }
    }
    // split the data up based on the final digits in the values
    return partition_set;
}

/* 
 * this function frees an array of partition sets
 */
void free_partition_sets(PartitionSet* partition_set, int num_partitions) {
    for (int i = 0; i < num_partitions; ++i) {
        // free things in this partition
        PartitionSet* current_partition = partition_set + i;
        free(current_partition->left_partition_values);
        free(current_partition->left_partition_positions);
        free(current_partition->right_partition_values);
        free(current_partition->right_partition_positions);
    }
    // free the partitions array
    free(partition_set);
    return;
}

/* 
 * this function inserts join data into the left and right results, 
 * keeping data in the join in the order of left column (which is arbitryary)
 * this requires shifting data in both columns
 */
void insert_to_result_and_maintain_order(int* left_result_column, int left_value, int* right_result_column, int right_value, int* num_results) {
    // keep data in the join in order of left column, requires shifting
    int correct_position = find_int_record_position(left_result_column, *num_results, left_value);
    // shift up from correct_position (both result arrays)
    shift_int_data_up(left_result_column, *num_results, correct_position, 1);
    shift_int_data_up(right_result_column, *num_results, correct_position, 1);
    // then add this value
    left_result_column[correct_position] = left_value;
    right_result_column[correct_position] = right_value;
    // increment number of results
    ++(*num_results);
    return;
}

/* 
 * this function handles a join using a hash join algorithm
 */
void db_join_hash(DbOperator* query, message* send_message) {
    log_info("db_join_hash\n");
    // select smaller column to be the one we hash
    int left_size = query->operator_fields.join_operator.val1_result->num_tuples;
    int right_size = query->operator_fields.join_operator.val2_result->num_tuples;
    // value information
    int* left_value_vector = (int*) query->operator_fields.join_operator.val1_result->payload;
    int* right_value_vector = (int*) query->operator_fields.join_operator.val2_result->payload;
    // positional information: need position vectors from the position bitvectors
    int* left_position_vector = pos_vector_from_bv((int*) query->operator_fields.join_operator.pos1_result->payload, query->operator_fields.join_operator.pos1_result->bitvector_ints);
    int* right_position_vector = pos_vector_from_bv((int*) query->operator_fields.join_operator.pos2_result->payload, query->operator_fields.join_operator.pos2_result->bitvector_ints);
    // eventual results
    int result_arr_size = STARTING_RESULT_CAPACITY;
    int* left_result_vector = malloc(sizeof(int) * result_arr_size);
    int* right_result_vector = malloc(sizeof(int) * result_arr_size);
    int num_results = 0;

    bool empty_join = false;
    // we might not have to do any work if it's a join on nothing
    if (left_size == 0 || right_size == 0) {
        empty_join = true;
    }

    if (!empty_join) {
        // TODO: recursively partition until we get the optimal size for the partitions?
        // partition prior to hashing and joining
        PartitionSet* partitions = generate_partitions(
            left_value_vector, left_position_vector, left_size,
            right_value_vector, right_position_vector, right_size,
            NUM_PARTITIONS);

        // grace hash join: partition and hash smaller partition, probe from larger partition
        // now perform a join on all of our partitions
        for (int p = 0; p < NUM_PARTITIONS; ++p) {
            PartitionSet* current_partition = partitions + p;
            // skip partition if there are no results on one side
            if (current_partition->num_entries_left_partition == 0 || current_partition->num_entries_right_partition == 0) {
                continue;
            }
            // organize the data stuctures for this partition
            int left_partition_size = current_partition->num_entries_left_partition;
            int right_partition_size = current_partition->num_entries_right_partition;
            // intermediate results for working
            int* to_iterate_value_vector = NULL;
            int* to_hash_value_vector = NULL;
            // need positional information
            int* to_iterate_pos_vector = NULL;
            int* to_hash_pos_vector = NULL;
            // need number of tuples
            int to_iterate_size;
            int to_hash_size;

            // assign to_iterate/to_hash based on partition column sizes
            if (left_partition_size > right_partition_size) {
                // partition specific stuff
                to_iterate_value_vector = current_partition->left_partition_values;
                to_iterate_pos_vector = current_partition->left_partition_positions;
                to_iterate_size = left_partition_size;
                to_hash_value_vector = current_partition->right_partition_values;
                to_hash_pos_vector = current_partition->right_partition_positions;
                to_hash_size = right_partition_size;
            } else {
                // partition specific stuff
                to_iterate_value_vector = current_partition->right_partition_values;
                to_iterate_pos_vector = current_partition->right_partition_positions;
                to_iterate_size = right_partition_size;
                to_hash_value_vector = current_partition->left_partition_values;
                to_hash_pos_vector = current_partition->left_partition_positions;
                to_hash_size = left_partition_size;
            }

            // hash the smaller column
            hashtable* ht;
            allocate(&ht, to_hash_size);
            for (int i = 0; i < to_hash_size; ++i) {
                // hash every value
                // value is the key, position is the value
                put(ht, to_hash_value_vector[i], to_hash_pos_vector[i]);
            }

            // iterate over the larger column, probe into the smaller
            for (int i = 0; i < to_iterate_size; ++i) {
                int probe_value = to_iterate_value_vector[i];
                int num_results_in_ht;
                int hashed_position;
                int* hashed_position_arr = NULL;
                int result = get(ht, probe_value, &hashed_position, 1, &num_results_in_ht);
                if (result == 0) {
                    // success
                    if (num_results_in_ht == 1) {
                        // success, found one result
                        // add this to our results arrays, get left and right result values
                        int left_value, right_value;
                        if (left_partition_size > right_partition_size) {
                            left_value = to_iterate_pos_vector[i]; // need the probed position
                            right_value = hashed_position;
                        } else {
                            left_value = hashed_position;
                            right_value = to_iterate_pos_vector[i]; // need the probed position
                        }
                        // insert while maintaining order, also increments num_results
                        insert_to_result_and_maintain_order(left_result_vector, left_value, right_result_vector, right_value, &num_results);
                        // resize results array if necessary
                        if (num_results == result_arr_size) {
                            int old_size = result_arr_size;
                            left_result_vector = resize_data(left_result_vector, &old_size);
                            // note we pass result_arr_size here, it's because this
                            // function resizes that parameter
                            right_result_vector = resize_data(right_result_vector, &result_arr_size);
                        }
                    } else if (num_results_in_ht > 1) {
                        // need to use an array for this
                        hashed_position_arr = malloc(sizeof(int) * num_results_in_ht);
                        // get the results again
                        int second_result = get(ht, probe_value, hashed_position_arr, num_results_in_ht, &num_results_in_ht);
                        assert(second_result == 0);
                        // now add all of these values
                        for (int j = 0; j < num_results_in_ht; ++j) {
                            // add this to our results arrays, get left and right result values
                            int left_value, right_value;
                            if (left_partition_size > right_partition_size) {
                                left_value = to_iterate_pos_vector[i]; // need the probed position
                                right_value = hashed_position_arr[j];
                            } else {
                                left_value = hashed_position_arr[j];
                                right_value = to_iterate_pos_vector[i]; // need the probed position
                            }
                            // insert while maintaining order, also increments num_results
                            insert_to_result_and_maintain_order(left_result_vector, left_value, right_result_vector, right_value, &num_results);
                            // resize results array if necessary
                            if (num_results == result_arr_size) {
                                int old_size = result_arr_size;
                                left_result_vector = resize_data(left_result_vector, &old_size);
                                // note we pass result_arr_size here, it's because this
                                // function resizes that parameter
                                right_result_vector = resize_data(right_result_vector, &result_arr_size);
                            }
                        }
                    } else {
                        log_info("found no matches in the hash table\n");
                    }
                } else {
                    // error
                    log_err("HASH TABLE ERROR\n");
                    abort();
                }
                // free the hashed_position_arr (we may or may not have used it)
                free(hashed_position_arr);
            }
            // free the hashtable
            deallocate(ht);
        }

        // finished with joining, free the partition data structures
        free_partition_sets(partitions, NUM_PARTITIONS);
    }

    // free position vectors
    free(left_position_vector);
    free(right_position_vector);

    // copy results into client context
    // create the left result object
    Result* left_result_obj = malloc(sizeof(Result));
    // no tuples in this result object
    left_result_obj->num_tuples = num_results;
    left_result_obj->payload = left_result_vector;
    left_result_obj->data_type = INT;
    left_result_obj->bitvector_ints = -1; // no bitvector ints, result is not a bitvector
    left_result_obj->is_posn_vector = true; // need to specify, because usually we use bitvectors
    // wrap the result object appropriately
    GeneralizedColumnHandle left_generalized_result_handle;
    // add name to result wrapper
    strcpy(left_generalized_result_handle.name, query->operator_fields.join_operator.left_handle);
    // update type
    left_generalized_result_handle.generalized_column.column_type = RESULT;
    // add the result
    left_generalized_result_handle.generalized_column.column_pointer.result = left_result_obj;
    // add this value to the client context variable pool
    add_to_client_context(query->context, left_generalized_result_handle);

    // create the right result object
    Result* right_result_obj = malloc(sizeof(Result));
    // no tuples in this result object
    right_result_obj->num_tuples = num_results;
    right_result_obj->payload = right_result_vector;
    right_result_obj->data_type = INT;
    right_result_obj->bitvector_ints = -1; // no bitvector ints, result is not a bitvector
    right_result_obj->is_posn_vector = true; // need to specify, because usually we use bitvectors
    // wrap the result object appropriately
    GeneralizedColumnHandle right_generalized_result_handle;
    // add name to result wrapper
    strcpy(right_generalized_result_handle.name, query->operator_fields.join_operator.right_handle);
    // update type
    right_generalized_result_handle.generalized_column.column_type = RESULT;
    // add the result
    right_generalized_result_handle.generalized_column.column_pointer.result = right_result_obj;
    // add this value to the client context variable pool
    add_to_client_context(query->context, right_generalized_result_handle);

    const char* result_message = "hash join successful";
    char* result_message_ptr = malloc(strlen(result_message) + 1);
    strcpy(result_message_ptr, result_message);
    send_message->payload = result_message_ptr;
    send_message->status = OK_DONE;

    return;
}
