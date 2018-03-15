#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include "cs165_api.h"
#include "utils.h"
#include "db_helpers.h"

#define STARTING_RESULT_CAPACITY 4096

/* 
 * this function finds the index of the first value greater than or equal to
 * low_value in a sorted array of data
 */
int sorted_column_index_gte(int* data, int num_records, int low_value) {
    assert(data);
    // binary search
    int start = 0;
    int end = num_records - 1;
    int middle_idx = -1;

    while (start <= end) {
        middle_idx = (start + end) / 2;
        int middle_val = data[middle_idx];

        if (low_value <= middle_val) {
            if (middle_idx == 0) {
                // everything is larger
                return middle_idx;
            } else if (data[middle_idx - 1] < low_value) {
                // this is the smallest index with values >= to low_value
                return middle_idx;
            }
            // recurse left
            end = middle_idx - 1;
        } else {
            // recurse right
            start = middle_idx + 1;
        }
    }

    return start;
}

/* 
 * this function finds the index of the last value less than high_value
 * in a sorted array of data
 */
int sorted_column_index_lt(int* data, int num_records, int high_value) {
    // binary search
    int start = 0;
    int end = num_records - 1;
    int middle_idx = -1;

    while (start <= end) {
        middle_idx = (start + end) / 2;
        int middle_val = data[middle_idx];

        if (middle_val < high_value) {
            if (middle_idx == num_records - 1) {
                // everything is smaller
                return middle_idx;
            } else if (data[middle_idx + 1] >= high_value) {
                // this is the largest index with values < high_value
                return middle_idx;
            }
            // recurse right
            start = middle_idx + 1;
        } else {
            // recurse left
            end = middle_idx - 1;
        }
    }

    return start;
}

/* 
 * this function creates a position vector of qualifying indices from a sorted
 * index of DataEntry's
 */
int* sorted_data_entry_select_range(DataEntry* data, int low_value, int high_value, int num_entries, int* num_records) {
    assert(data);

    // things to eventually return/update for the caller
    int current_pos_vector_sz = STARTING_RESULT_CAPACITY;
    int* position_vector = malloc(current_pos_vector_sz * sizeof(int));
    int results_count = 0;

    // binary search
    int start = 0;
    int end = num_entries - 1;
    int middle_idx = -1;

    while (start <= end) {
        middle_idx = (start + end) / 2;
        int middle_val = data[middle_idx].value;

        if (low_value <= middle_val) {
            if (middle_idx == 0) {
                // everything is larger
                start = middle_idx;
                break;
            } else if (data[middle_idx - 1].value < low_value) {
                // this is the smallest index with values >= to low_value
                start = middle_idx;
                break;
            }
            // recurse left
            end = middle_idx - 1;
        } else {
            // recurse right
            start = middle_idx + 1;
        }
    }
    // now start is the starting index of qualifying values

    int current_idx = start;
    while (data[current_idx].value < high_value) {
        // check if we need to resize our position_vector
        if (results_count == current_pos_vector_sz) {
            // resize
            position_vector = resize_data(position_vector, &current_pos_vector_sz);
        }
        // add this position and increment the number of results
        position_vector[results_count++] = data[current_idx].pos;
        // increment the current idx in our DataEntry's
        ++current_idx;
    }

    // set the number of results
    *num_records = results_count;
    return position_vector;
}

/* 
 * this function takes a start and end value, and marks the values in between
 * them (inclusive) as present (1) in the bit vector result
 */
void mark_bv_range(int* result, int start_idx, int end_idx) {
    // how many bits do we need to mark
    // +1 because these are inclusive indexes
    int num_bits_to_mark = end_idx - start_idx + 1;
    // get the first chunk of bits that needs changing
    int next_index_to_mark = start_idx / BITS_PER_INT;
    // mark the initial values in this int
    // need to mark the end of this chunk of bits, find out how many need
    // to be shifted to do this
    int num_first_bits_to_shift = start_idx % BITS_PER_INT;
    // this takes care of the shift left, i.e. 111110000000000
    int mask = ones_mask_from_right(BITS_PER_INT - num_first_bits_to_shift);
    // track how many things we'll be marking
    int num_bits_already_marked = BITS_PER_INT - num_first_bits_to_shift;
    // TODO: take care of trimming the mask if necessary
    if (num_bits_already_marked > num_bits_to_mark) {
        // add a mask like so 000001111111111
        int num_to_trim = num_bits_already_marked - num_bits_to_mark;
        int needed_for_mask = BITS_PER_INT - num_to_trim;
        int mask_left = ones_mask_from_left(needed_for_mask);
        mask = mask & mask_left;
        assert(num_bits_already_marked - num_to_trim == num_bits_to_mark);
        num_bits_already_marked -= num_to_trim;
    }

    // mark that first int
    result[next_index_to_mark] = result[next_index_to_mark] | mask;
    ++next_index_to_mark;
    // update number of bits remaining to update
    // subtract sizeof(int) from num_first_bits_to_shift since that is how
    // many we marked
    num_bits_to_mark -= num_bits_already_marked;;
    if (num_bits_to_mark > 0) {
        // now figure out how many more ints need to be fully marked
        int num_ints_to_fully_mark = num_bits_to_mark / BITS_PER_INT;
        // dont mark final_int_to_mark, go up to one before it
        int final_int_to_mark = next_index_to_mark + num_ints_to_fully_mark;
        for (int i = next_index_to_mark; i < final_int_to_mark; ++i) {
            // mark them all
            result[i] = -1;
            // update number of bits remaining to mark
            num_bits_to_mark -= BITS_PER_INT;
            // update next index to mark
            ++next_index_to_mark;
        }
    }
    if (num_bits_to_mark > 0) {
        // now mark the final int - mark the first num_bits_to_mark bits
        result[next_index_to_mark] = result[next_index_to_mark] | ones_mask_from_left(num_bits_to_mark);
    }
    return;
}

/* 
 * this function marks a bitvector using the qualifying positions from a
 * position vector
 */
void mark_bv_from_pos_vec(int* result, int* pos_vec, int num_results) {
    for (int i = 0; i < num_results; ++i) {
        int next_pos_to_mark = pos_vec[i];
        // find the int that we need to mark
        int int_idx_to_mark = next_pos_to_mark / BITS_PER_INT;
        // mark that result
        result[int_idx_to_mark] = result[int_idx_to_mark] | (1 << (next_pos_to_mark % BITS_PER_INT));
    }
    return;
}

/* 
 * this function returns a bool indicating whether or not a position is marked
 * in a bitvector
 */
bool check_bv_position(int* bv, int pos) {
    int int_pos = pos / BITS_PER_INT;
    return bv[int_pos] & (1 << (pos % BITS_PER_INT));
}

/* 
 * this function takes a bitvector and returns a position vector representing it
 */
int* pos_vector_from_bv(int* bv, int num_bv_ints) {
    int result_size = STARTING_RESULT_CAPACITY;
    int* position_vector = malloc(sizeof(int) * result_size);
    int num_position_vector_results = 0;

    int global_count = 0;
    for (int i = 0; i < num_bv_ints; ++i) {
        int current_bv_int = bv[i];
        for (size_t j = 0; j < BITS_PER_INT; ++j) {
            if (current_bv_int & (1 << j)) {
                position_vector[num_position_vector_results++] = global_count;
            }
            ++global_count;
            if (num_position_vector_results == result_size) {
                // double the size
                // get the old size and results
                int old_result_size = result_size;
                int* old_position_vector = position_vector;
                // double result size
                result_size = 2 * old_result_size;
                // create new position vector
                position_vector = malloc(sizeof(int) * result_size);
                // copy old data over
                memcpy(position_vector, old_position_vector, old_result_size * sizeof(int));
                // free old position vector
                free(old_position_vector);
            }
        }
    }
    return position_vector;
}
