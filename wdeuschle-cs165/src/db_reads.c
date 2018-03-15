#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include "cs165_api.h"
#include "utils.h"
#include "client_context.h"
#include "db_reads_indexed.h"
#include "btree.h"

/*
 * This function takes an array of integers, the quantity of them, their
 * length, and returns them in a stringified format.
 */
char* convert_int_results_to_string(int* results, int num_results, char delim) {
    // signed ints can be 11 characters max, plus an extra two
    // characters for a space and a comma. also 1 for the null terminator
    char* results_str = malloc(num_results * 13 + 1);
    char* current_results_ptr = results_str;
    for (int i = 0; i < num_results; i++) {
        int current_result = results[i];
        // handle negative result
        if (current_result < 0) {
            current_result = -1 * current_result;
            current_results_ptr[0] = '-';
            current_results_ptr++;
        }
        char* str_int = itoa(current_result, 10);
        int str_int_length = strlen(str_int);
        memcpy(current_results_ptr, str_int, str_int_length);
        current_results_ptr += str_int_length;
        current_results_ptr[0] = delim;
        current_results_ptr++;
    }
    // null terminate
    current_results_ptr[-1] = '\0';
    return results_str;
}

/*
 * This function takes an array of integers, the quantity of them, their
 * length, and returns them in a stringified format.
 */
char* convert_long_results_to_string(long* results, int num_results, char delim) {
    // signed longs can be 20 characters max, plus an extra two
    // characters for a space and a comma. also 1 for the null terminator
    char* results_str = malloc(num_results * 22 + 1);
    char* current_results_ptr = results_str;
    for (int i = 0; i < num_results; i++) {
        long current_result = results[i];
        // handle negative result
        if (current_result < 0) {
            current_result = -1 * current_result;
            current_results_ptr[0] = '-';
            current_results_ptr++;
        }
        char* str_int = itoa(current_result, 10);
        int str_int_length = strlen(str_int);
        memcpy(current_results_ptr, str_int, str_int_length);
        current_results_ptr += str_int_length;
        current_results_ptr[0] = delim;
        current_results_ptr++;
    }
    // null terminate
    current_results_ptr[-1] = '\0';
    return results_str;
}

/*
 * This function takes a double and returns it in a stringified format.
 */
char* convert_double_result_to_string(double result) {
    char* char_res = malloc(50 * sizeof(char));
    snprintf(char_res, 50, "%.2f", result);
    return char_res;
}

void db_select_bitvector(DbOperator* query, message* send_message) {
    // BITVECTOR VERSION
    // timing
    // ******************
    clock_t start, end;
    double cpu_time_used;
    start = clock();
    // ******************
    
    log_info("calling db_select_bitvector\n");
    int num_entries = query->operator_fields.select_operator.num_results;
    int* column_data;

    // indexing data
    bool using_index = false;
    IndexType index_type = NO_INDEX;
    void* index = NULL;
    bool clustered = false;

    if (query->operator_fields.select_operator.compare_info->gen_col.column_type == COLUMN) {
        Column* column = query->operator_fields.select_operator.compare_info->gen_col.column_pointer.column;
        column_data = column->data;
        // do we have an index
        if (column->index_type == SORTED) {
            using_index = true;
            index_type = SORTED;
            index = column->index;
            clustered = column->clustered;
        } else if (column->index_type == BTREE) {
            using_index = true;
            index_type = BTREE;
            index = column->index;
            clustered = column->clustered;
        }
    } else {
        column_data = query->operator_fields.select_operator.compare_info->gen_col.column_pointer.result->payload;
    }
    // for comparisons
    int low_value = query->operator_fields.select_operator.compare_info->p_low;
    int high_value = query->operator_fields.select_operator.compare_info->p_high;
    int num_results = 0;

    // NEW: create a bitvector here
    int num_ints_needed;
    int* result;

    if (query->operator_fields.select_operator.compare_info->has_posn_vector == true) {
        // only compare elements in the posn vector
        int num_posn_entries = query->operator_fields.select_operator.num_posn_results;
        int* bv_posn_data = query->operator_fields.select_operator.compare_info->posn_vector.column_pointer.result->payload;
        int num_bitvector_ints = query->operator_fields.select_operator.compare_info->posn_vector.column_pointer.result->bitvector_ints;
        // NEW: create a bitvector here
        // going to create a new bit vector off of the same column data
        // so just repeat the size of the bitvector that we are using for
        // our selections
        num_ints_needed = num_bitvector_ints;
        result = calloc(num_ints_needed, sizeof(int));
        // use posn vector for results, comparison is applied to column_data
        // convert from bitvector to normal position vector
        int* posn_data = bitvector_to_vector(bv_posn_data, num_bitvector_ints, num_entries);
        for (int i = 0; i < num_posn_entries; i++) {
            int val = column_data[i];
            if (val >= low_value && val < high_value) {
                // store the indices as the result
                // get the actual index
                int idx = posn_data[i];
                // convert to bitvector form
                int int_idx = idx / BITS_PER_INT;
                int int_offset = idx % BITS_PER_INT;
                result[int_idx] = (result[int_idx] | (1 << int_offset));
                ++num_results;
            }
        }
        // free the positional data
        free(posn_data);
    } else {
        num_ints_needed = num_bitvector_ints_needed(num_entries);
        result = calloc(num_ints_needed, sizeof(int));
        // can we use an index
        if (using_index) {
            if (index_type == SORTED) {
                log_info("selecting with sorted index\n");
                if (clustered) {
                    // search for the start and end indexes, and then mark
                    // our bitvector accordingly
                    int start_idx = sorted_column_index_gte(column_data, num_entries, low_value);
                    int end_idx = sorted_column_index_lt(column_data, num_entries, high_value);
                    // plus 1 because these are inclusive indexes
                    num_results = end_idx - start_idx + 1;
                    mark_bv_range(result, start_idx, end_idx);
                } else {
                    // create position vector from unclustered sorted index
                    int* pos_vec = sorted_data_entry_select_range((DataEntry*) index, low_value, high_value, num_entries, &num_results);
                    mark_bv_from_pos_vec(result, pos_vec, num_results);
                    // free the pos vec
                    free(pos_vec);
                }
            } else {
                log_info("selecting with btree index\n");
                if (clustered) {
                    log_info("SELECTED WITH CLUSTERED BTREE INDEX\n");
                    // TODO: can speed this by selecting for values at the endpoints
                    // unclustered btree
                    int* pos_vec = btree_select_range((BTree*) index, low_value, high_value, &num_results);
                    mark_bv_from_pos_vec(result, pos_vec, num_results);
                    // free the pos vec
                    free(pos_vec);
                } else {
                    // unclustered btree
                    int* pos_vec = btree_select_range((BTree*) index, low_value, high_value, &num_results);
                    mark_bv_from_pos_vec(result, pos_vec, num_results);
                    // free the pos vec
                    free(pos_vec);
                }
            }
        } else {
            for (int i = 0; i < num_entries; i++) {
                int val = column_data[i];
                if (val >= low_value && val < high_value) {
                    // update the bitvector
                    // get the right char
                    int int_idx = i / BITS_PER_INT;
                    int int_offset = i % BITS_PER_INT;
                    result[int_idx] = (result[int_idx] | (1 << int_offset));
                    ++num_results;
                }
            }
        }
    }

    // create the result object
    Result* result_obj = malloc(sizeof(Result));
    result_obj->num_tuples = num_results;
    result_obj->payload = result;
    result_obj->data_type = INT;
    result_obj->bitvector_ints = num_ints_needed;
    result_obj->is_posn_vector = false; // this is a bitvector

    // wrap the result object appropriately
    // TODO: malloc here?
    GeneralizedColumnHandle generalized_result_handle;// = malloc(sizeof(GeneralizedColumnHandle));
    // add name to result wrapper
    strcpy(generalized_result_handle.name, query->operator_fields.select_operator.compare_info->handle);
    // update type
    generalized_result_handle.generalized_column.column_type = RESULT;
    // add the result
    generalized_result_handle.generalized_column.column_pointer.result = result_obj;

    // add this value to the client context variable pool
    add_to_client_context(query->context, generalized_result_handle);

    const char* result_message = "select successful";
    char* result_message_ptr = malloc(strlen(result_message) + 1);
    strcpy(result_message_ptr, result_message);
    send_message->payload = result_message_ptr;
    send_message->status = OK_DONE;

    // timing
    // ******************
    end = clock();
    cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
    log_timing("time to select: %f\n", cpu_time_used);
    // ******************
    
    return;
}

/*
 * This function takes a DbOperator object and selects the specified items.
 * It stores a status result in send_message.
 */
void db_select_columnwise(DbOperator* query, message* send_message) {
    // NORMAL VERSION
    // timing
    // ******************
    clock_t start, end;
    double cpu_time_used;
    start = clock();
    // ******************
    
    log_info("calling db_select_columnwise\n");
    int num_entries = query->operator_fields.select_operator.num_results;
    int* column_data;
    if (query->operator_fields.select_operator.compare_info->gen_col.column_type == COLUMN) {
        column_data = query->operator_fields.select_operator.compare_info->gen_col.column_pointer.column->data;
    } else {
        column_data = query->operator_fields.select_operator.compare_info->gen_col.column_pointer.result->payload;
    }
    // for comparisons
    int low_value = query->operator_fields.select_operator.compare_info->p_low;
    int high_value = query->operator_fields.select_operator.compare_info->p_high;
    int num_results = 0;
    // TODO: resize results somehow?
    int* result = malloc(num_entries * sizeof(int));

    if (query->operator_fields.select_operator.compare_info->has_posn_vector == true) {
        // only compare elements in the posn vector
        int num_posn_entries = query->operator_fields.select_operator.num_posn_results;
        int* posn_data;
        if (query->operator_fields.select_operator.compare_info->posn_vector.column_type == COLUMN) {
            posn_data = query->operator_fields.select_operator.compare_info->posn_vector.column_pointer.column->data;
        } else {
            posn_data = query->operator_fields.select_operator.compare_info->posn_vector.column_pointer.result->payload;
        }
        // use posn vector for results, comparison is applied to column_data
        for (int i = 0; i < num_posn_entries; i++) {
            int val = column_data[i];
            log_timing("val: %d\n", val);
            if (val >= low_value && val < high_value) {
                // store the indices as the result
                result[num_results++] = posn_data[i];
                log_timing("values we should get: %d\n", result[num_results - 1]);
            }
        }
    } else {
        for (int i = 0; i < num_entries; i++) {
            int val = column_data[i];
            if (val >= low_value && val < high_value) {
                // store the indices as the result
                result[num_results++] = i;
            }
        }
    }

    // create the result object
    Result* result_obj = malloc(sizeof(Result));
    result_obj->num_tuples = num_results;
    result_obj->payload = result;
    result_obj->data_type = INT;
    result_obj->is_posn_vector = true;

    // wrap the result object appropriately
    // TODO: malloc here?
    GeneralizedColumnHandle generalized_result_handle;// = malloc(sizeof(GeneralizedColumnHandle));
    // add name to result wrapper
    strcpy(generalized_result_handle.name, query->operator_fields.select_operator.compare_info->handle);
    // update type
    generalized_result_handle.generalized_column.column_type = RESULT;
    // add the result
    generalized_result_handle.generalized_column.column_pointer.result = result_obj;

    // add this value to the client context variable pool
    add_to_client_context(query->context, generalized_result_handle);

    const char* result_message = "select successful";
    char* result_message_ptr = malloc(strlen(result_message) + 1);
    strcpy(result_message_ptr, result_message);
    send_message->payload = result_message_ptr;
    send_message->status = OK_DONE;

    // timing
    // ******************
    end = clock();
    cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
    log_timing("time to select: %f\n", cpu_time_used);
    // ******************
    
    return;
}

/* 
 * this function dispatches the correct select function based on global flags
 */
void db_select(DbOperator* query, message* send_message) {
    if (BITVECTOR_DB) {
        db_select_bitvector(query, send_message);
    } else {
        db_select_columnwise(query, send_message);
    }
}

/* 
 * this aggregrates the results of a select operation, returns status
 * of the operation in send_message
 */
void db_fetch_columnwise(DbOperator* query, message* send_message) {
    // NORMAL VERSION
    // timing
    // ******************
    clock_t start, end;
    double cpu_time_used;
    start = clock();
    // ******************

    // TODO: more error checking
    log_info("calling db_fetch_columnwise\n");
    // access necessary items
    int num_results = query->operator_fields.fetch_operator.ids_result->num_tuples;
    int* ids_result = query->operator_fields.fetch_operator.ids_result->payload;
    int* column = query->operator_fields.fetch_operator.column->data;

    // result array
    int* result = malloc(num_results * sizeof(int));

    // for every id in the result
    for (int i = 0; i < num_results; ++i) {
        // access the corresponding value
        result[i] = column[ids_result[i]];
    }

    // create the result object
    Result* result_obj = malloc(sizeof(Result));
    result_obj->num_tuples = num_results;
    result_obj->payload = result;
    result_obj->data_type = INT;
    result_obj->is_posn_vector = true; // this is a position vector because it's a fetch

    // wrap the result object appropriately
    // TODO: malloc here?
    GeneralizedColumnHandle generalized_result_handle;// = malloc(sizeof(GeneralizedColumnHandle));
    log_info("do we have a handle: %s\n", query->operator_fields.fetch_operator.handle);
    // add name to result wrapper
    strcpy(generalized_result_handle.name, query->operator_fields.fetch_operator.handle);
    // update type
    generalized_result_handle.generalized_column.column_type = RESULT;
    // add the result
    generalized_result_handle.generalized_column.column_pointer.result = result_obj;

    // add this value to the client context variable pool
    add_to_client_context(query->context, generalized_result_handle);

    char* result_message = "fetch successful";
    char* result_message_ptr = malloc(strlen(result_message) + 1);
    strcpy(result_message_ptr, result_message);
    send_message->payload = result_message_ptr;
    send_message->status = OK_DONE;

    // timing
    // ******************
    end = clock();
    cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
    log_timing("time to fetch: %f\n", cpu_time_used);
    // ******************

    return;
}

void db_fetch_bitvector(DbOperator* query, message* send_message) {
    // BITVECTOR VERSION

    // TODO: more error checking
    log_info("calling db_fetch_bitvector\n");
    // access necessary items
    int num_results = query->operator_fields.fetch_operator.ids_result->num_tuples;
    int num_bitvector_ints = query->operator_fields.fetch_operator.ids_result->bitvector_ints;
    int* ids_result = query->operator_fields.fetch_operator.ids_result->payload;
    int* column = query->operator_fields.fetch_operator.column->data;

    // result array
    // add one so that we can write loops with fewer branch predictions
    int* result = malloc((num_results + 1) * sizeof(int));

    int current_idx = 0;
    // for every id in the result
    for (int i = 0; i < num_bitvector_ints; ++i) {
        unsigned int current_int = ids_result[i];
        if (current_int == 0) {
            continue;
        }
        for (int j = 0; j < (int) BITS_PER_INT; ++j) {
            if ((current_int & (1 << j)) != 0) {
                result[current_idx] = column[(i * BITS_PER_INT) + j];
                current_idx += 1;
            }
        }
    }

    // create the result object
    Result* result_obj = malloc(sizeof(Result));
    result_obj->num_tuples = num_results;
    result_obj->payload = result;
    result_obj->data_type = INT;
    // NOTE: this is the number of bitvector ints used to perform the fetch,
    // but this result is itself values
    result_obj->bitvector_ints = num_bitvector_ints;
    result_obj->is_posn_vector = true; // this isn't a bitvector, it's the results from a fetch

    // wrap the result object appropriately
    // TODO: malloc here?
    GeneralizedColumnHandle generalized_result_handle;// = malloc(sizeof(GeneralizedColumnHandle));
    log_info("do we have a handle: %s\n", query->operator_fields.fetch_operator.handle);
    // add name to result wrapper
    strcpy(generalized_result_handle.name, query->operator_fields.fetch_operator.handle);
    // update type
    generalized_result_handle.generalized_column.column_type = RESULT;
    // add the result
    generalized_result_handle.generalized_column.column_pointer.result = result_obj;

    // add this value to the client context variable pool
    add_to_client_context(query->context, generalized_result_handle);

    char* result_message = "fetch successful";
    char* result_message_ptr = malloc(strlen(result_message) + 1);
    strcpy(result_message_ptr, result_message);
    send_message->payload = result_message_ptr;
    send_message->status = OK_DONE;
    return;
}

/* 
 * this function dispatches the correct fetch function based on global flags
 */
void db_fetch(DbOperator* query, message* send_message) {
    if (BITVECTOR_DB && query->operator_fields.fetch_operator.ids_result->is_posn_vector != true) {
        db_fetch_bitvector(query, send_message);
    } else {
        db_fetch_columnwise(query, send_message);
    }
}

/* 
 * this sums the values in a column
 */
long sum(int* arr, int num_entries) {
    long total = 0;
    for (int i = 0; i < num_entries; ++i) {
        total += arr[i];
    }
    return total;
}

/* 
 * this sums the results in a column
 */
void db_sum(DbOperator* query, message* send_message) {
    log_info("calling db_sum\n");
    // for computing the sum
    long total_tally = 0;
    int* results;
    int num_results = query->operator_fields.sum_operator.num_results;

    if (query->operator_fields.sum_operator.generalized_column.column_type == RESULT) {
        // result type
        results = (int*) query->operator_fields.sum_operator.generalized_column.column_pointer.result->payload;
    } else {
        // column type
        results = (int*) query->operator_fields.sum_operator.generalized_column.column_pointer.column->data;
    }
    // sum the results
    total_tally = sum(results, num_results);

    // return res
    long* total = malloc(1 * sizeof(long)); 
    *total = total_tally;

    // create the result obj
    Result* result_obj = malloc(sizeof(Result));
    // account for situations where we are adding 0 elements
    if (num_results == 0) {
        result_obj->num_tuples = 0;
    } else {
        result_obj->num_tuples = 1;
    }
    result_obj->payload = total;
    result_obj->data_type = LONG;
    result_obj->is_posn_vector = false;

    // wrap the results appropriately
    GeneralizedColumnHandle result_wrapper;
    // add name, total to result_wrapper
    strcpy(result_wrapper.name, query->operator_fields.sum_operator.handle);
    // update type
    result_wrapper.generalized_column.column_type = RESULT;
    // add the result
    result_wrapper.generalized_column.column_pointer.result = result_obj;
    // add this value to the client context variable pool
    add_to_client_context(query->context, result_wrapper);
    log_info("SUM: %li\n", *total);

    const char* result_message = "sum successful";
    char* result_message_ptr = malloc(strlen(result_message) + 1);
    strcpy(result_message_ptr, result_message);
    send_message->payload = result_message_ptr;
    send_message->status = OK_DONE;
    return;
}

/* 
 * this find the min in a column
 */
void db_min(DbOperator* query, message* send_message) {
    log_info("calling db_min\n");

    int* final_result = malloc(sizeof(int) * 1);
    int *arr;
    int num_results = query->operator_fields.min_operator.num_results;
    if (query->operator_fields.min_operator.generalized_column.column_type == RESULT) {
        // result type
        arr = (int*) query->operator_fields.min_operator.generalized_column.column_pointer.result->payload;
    } else {
        // column type
        arr = (int*) query->operator_fields.min_operator.generalized_column.column_pointer.column->data;
    }

    // initialize it as the first element
    *final_result = arr[0];
    for (int i = 1; i < num_results; ++i) {
        if (arr[i] < *final_result) {
            *final_result = arr[i];
        }
    }

    // create the result obj
    Result* result_obj = malloc(sizeof(Result));
    result_obj->num_tuples = 1;
    result_obj->payload = final_result;
    result_obj->data_type = INT;
    result_obj->is_posn_vector = false;

    // wrap the results appropriately
    GeneralizedColumnHandle result_wrapper;
    // add name, total to result_wrapper
    strcpy(result_wrapper.name, query->operator_fields.min_operator.handle);
    // update type
    result_wrapper.generalized_column.column_type = RESULT;
    // add the result
    result_wrapper.generalized_column.column_pointer.result = result_obj;
    // add this value to the client context variable pool
    add_to_client_context(query->context, result_wrapper);
    log_info("MIN: %d\n", final_result);

    const char* result_message = "min successful";
    char* result_message_ptr = malloc(strlen(result_message) + 1);
    strcpy(result_message_ptr, result_message);
    send_message->payload = result_message_ptr;
    send_message->status = OK_DONE;
    return;
}

/* 
 * this find the max in a column
 */
void db_max(DbOperator* query, message* send_message) {
    log_info("calling db_max\n");

    int* final_result = malloc(sizeof(int) * 1);
    int *arr;
    int num_results = query->operator_fields.max_operator.num_results;
    if (query->operator_fields.max_operator.generalized_column.column_type == RESULT) {
        // result type
        arr = (int*) query->operator_fields.max_operator.generalized_column.column_pointer.result->payload;
    } else {
        // column type
        arr = (int*) query->operator_fields.max_operator.generalized_column.column_pointer.column->data;
    }

    // initialize it as the first element
    *final_result = arr[0];
    for (int i = 1; i < num_results; ++i) {
        if (arr[i] > *final_result) {
            *final_result = arr[i];
        }
    }

    // create the result obj
    Result* result_obj = malloc(sizeof(Result));
    result_obj->num_tuples = 1;
    result_obj->payload = final_result;
    result_obj->data_type = INT;
    result_obj->is_posn_vector = false;

    // wrap the results appropriately
    GeneralizedColumnHandle result_wrapper;
    // add name, total to result_wrapper
    strcpy(result_wrapper.name, query->operator_fields.max_operator.handle);
    // update type
    result_wrapper.generalized_column.column_type = RESULT;
    // add the result
    result_wrapper.generalized_column.column_pointer.result = result_obj;
    // add this value to the client context variable pool
    add_to_client_context(query->context, result_wrapper);
    log_info("MAX: %d\n", final_result);

    const char* result_message = "max successful";
    char* result_message_ptr = malloc(strlen(result_message) + 1);
    strcpy(result_message_ptr, result_message);
    send_message->payload = result_message_ptr;
    send_message->status = OK_DONE;
    return;
}

/* 
 * this averages the results in a column
 */
void db_average(DbOperator* query, message* send_message) {
    log_info("calling db_average\n");
    // for computing the average
    long total = 0;
    int* results;
    int num_results = query->operator_fields.average_operator.num_results;

    if (query->operator_fields.average_operator.generalized_column.column_type == RESULT) {
        // result type
        results = (int*) query->operator_fields.average_operator.generalized_column.column_pointer.result->payload;
    } else {
        // column type
        results = (int*) query->operator_fields.average_operator.generalized_column.column_pointer.column->data;
    }
    // sum the results
    total = sum(results, num_results);
    log_info("TOTAL: %d\n", total);
    log_info("NUMBER OF RESULTS: %d\n", num_results);

    // average
    double* average = malloc(1 * sizeof(double)); 
    *average = (long double) total / (double) num_results;

    // create the result obj
    Result* result_obj = malloc(sizeof(Result));
    result_obj->num_tuples = 1;
    result_obj->payload = average;
    result_obj->data_type = DOUBLE;
    result_obj->is_posn_vector = false;

    // wrap the results appropriately
    GeneralizedColumnHandle result_wrapper;
    // add name, average to result_wrapper
    strcpy(result_wrapper.name, query->operator_fields.average_operator.handle);
    // update type
    result_wrapper.generalized_column.column_type = RESULT;
    // add the result
    result_wrapper.generalized_column.column_pointer.result = result_obj;
    // add this value to the client context variable pool
    add_to_client_context(query->context, result_wrapper);
    log_info("AVERAGE: %f\n", *average);

    const char* result_message = "average successful";
    char* result_message_ptr = malloc(strlen(result_message) + 1);
    strcpy(result_message_ptr, result_message);
    send_message->payload = result_message_ptr;
    send_message->status = OK_DONE;
    return;
}

/* 
 * this adds the results in two columns
 */
void db_add(DbOperator* query, message* send_message, int mult_factor) {
    log_info("calling db_add\n");

    // create results array, assuming they are the same size
    int num_results = query->operator_fields.add_operator.num_results1;
    int* arr1;
    int* arr2;
    // populate both arr pointers
    if (query->operator_fields.add_operator.generalized_column1.column_type == RESULT) {
        // result type
        arr1 = (int*) query->operator_fields.add_operator.generalized_column1.column_pointer.result->payload;
    } else {
        // column type
        arr1 = (int*) query->operator_fields.add_operator.generalized_column1.column_pointer.column->data;
    }
    if (query->operator_fields.add_operator.generalized_column2.column_type == RESULT) {
        // result type
        arr2 = (int*) query->operator_fields.add_operator.generalized_column2.column_pointer.result->payload;
    } else {
        // column type
        arr2 = (int*) query->operator_fields.add_operator.generalized_column2.column_pointer.column->data;
    }

    int* final_results = malloc(sizeof(int) * num_results);

    for (int i = 0; i < num_results; ++i) {
        final_results[i] = arr1[i] + (mult_factor * arr2[i]);
    }

    // create the result obj
    Result* result_obj = malloc(sizeof(Result));
    result_obj->num_tuples = num_results;
    result_obj->payload = final_results;
    result_obj->data_type = INT;
    result_obj->is_posn_vector = false;

    // wrap the results appropriately
    GeneralizedColumnHandle result_wrapper;
    // add name, total to result_wrapper
    strcpy(result_wrapper.name, query->operator_fields.add_operator.handle);
    // update type
    result_wrapper.generalized_column.column_type = RESULT;
    // add the result
    result_wrapper.generalized_column.column_pointer.result = result_obj;
    // add this value to the client context variable pool
    add_to_client_context(query->context, result_wrapper);

    const char* result_message = "add successful";
    char* result_message_ptr = malloc(strlen(result_message) + 1);
    strcpy(result_message_ptr, result_message);
    send_message->payload = result_message_ptr;
    send_message->status = OK_DONE;
    return;
}

/* 
 * this function serializes and returns the results of some previous operation
 * to the client. stores these results in the payload of send_message
 */
void serialized_db_print(DbOperator* query, message* send_message) {
    // TODO: more error checking
    log_info("calling db_print\n");

    int num_handles = query->operator_fields.print_operator.num_results;

    char* results_strs[num_handles];

    for (int i = 0; i < num_handles; ++i) {
        // access necessary items
        int num_results = query->operator_fields.print_operator.results[i]->num_tuples;
        log_info("num results: %d\n", num_results);

        // use correct data type
        int data_type = query->operator_fields.print_operator.results[i]->data_type;
        char* results_str;

        if (data_type == INT) {
            int* print_result = query->operator_fields.print_operator.results[i]->payload;
            // send it back as a string (for now)
            results_str = convert_int_results_to_string(print_result, num_results, '\n');
            send_message->status = OK_WAIT_FOR_RESPONSE;
        } else if (data_type == LONG) {
            long* print_result = query->operator_fields.print_operator.results[i]->payload;
            // send it back as a string (for now)
            results_str = convert_long_results_to_string(print_result, num_results, '\n');
            send_message->status = OK_WAIT_FOR_RESPONSE;
        } else if (data_type == DOUBLE) {
            double* print_result = query->operator_fields.print_operator.results[i]->payload;
            results_str = convert_double_result_to_string(*print_result);
            send_message->status = OK_WAIT_FOR_RESPONSE;
        } else {
            const char* result_message = "print failed, check the data type of the result\n";
            results_str = malloc(strlen(result_message) + 1);
            strcpy(results_str, result_message);
            send_message->status = EXECUTION_ERROR;
        }

        results_strs[i] = results_str;
    }

    // figure out length of final string, initial num_handles is for the additional commas we'll need
    int final_str_len = num_handles;
    for (int i = 0; i < num_handles; ++i) {
        final_str_len += strlen(results_strs[i]);
    }

    char* final_str = malloc(sizeof(char) * final_str_len);
    char* track_final_str = final_str;

    for (int i = 0; i < num_handles; ++i) {
        int next_val_len = strlen(results_strs[i]);
        strcpy(track_final_str, results_strs[i]);
        free(results_strs[i]);
        track_final_str = track_final_str + next_val_len;
        // don't do this for the last one
        if (i < num_handles - 1) {
            track_final_str[0] = ',';
            ++track_final_str;
        }
    }

    /*log_info("final print str: %s\n", final_str);*/
    send_message->payload = final_str;
    return;
}

/* 
 * this function sends data to the client, usually in the form of binary which
 * the client can then interpret. if we are mixing data types in the response,
 * we will serialize it on the server before sending as ascii to the client
 */
void db_print(DbOperator* query, message* send_message) {
    log_info("calling new db_print\n");

    // access necessary items
    // num_handles is the same as the number of columns being sent back
    int num_handles = query->operator_fields.print_operator.num_results;
    log_info("num handles: %d\n", num_handles);
    int num_results = query->operator_fields.print_operator.results[0]->num_tuples;
    log_info("num results: %d\n", num_results);
    int total_data_items = num_handles * num_results;

    // track number of columns
    send_message->num_columns = num_handles;
    // use correct data type
    send_message->data_type = query->operator_fields.print_operator.results[0]->data_type;
    // make sure this isn't a mixed data type
    for (int i = 0; i < num_handles; ++i) {
        if (send_message->data_type != query->operator_fields.print_operator.results[i]->data_type) {
            // mixed data type, use our serialization print method
            serialized_db_print(query, send_message);
            return;
        }
    }

    // weave the results together
    if (send_message->data_type == INT) {
        int* payload = malloc(total_data_items * sizeof(int));
        for (int i = 0; i < num_handles; ++i) {
            int* print_vals = (int*) query->operator_fields.print_operator.results[i]->payload;
            for (int j = 0; j < num_results; ++j) {
                payload[(j * num_handles) + i] = print_vals[j];
            }
        }
        // new
        send_message->status = OK_WAIT_FOR_DATA;
        send_message->payload = (char*) payload;
        send_message->length = total_data_items * sizeof(int);
    } else if (send_message->data_type == LONG) {
        long* payload = malloc(total_data_items * sizeof(long));
        for (int i = 0; i < num_handles; ++i) {
            long* print_vals = (long*) query->operator_fields.print_operator.results[i]->payload;
            for (int j = 0; j < num_results; ++j) {
                payload[(j * num_handles) + i] = print_vals[j];
            }
        }
        // new
        send_message->status = OK_WAIT_FOR_DATA;
        send_message->payload = (char*) payload;
        send_message->length = total_data_items * sizeof(long);
    } else if (send_message->data_type == DOUBLE) {
        double* payload = malloc(total_data_items * sizeof(double));
        for (int i = 0; i < num_handles; ++i) {
            double* print_vals = (double*) query->operator_fields.print_operator.results[i]->payload;
            for (int j = 0; j < num_results; ++j) {
                payload[(j * num_handles) + i] = print_vals[j];
            }
        }
        // new
        send_message->status = OK_WAIT_FOR_DATA;
        send_message->payload = (char*) payload;
        send_message->length = total_data_items * sizeof(double);
    } else {
        const char* result_message = "print failed, check the data type of the result\n";
        char* results_str = malloc(strlen(result_message) + 1);
        strcpy(results_str, result_message);
        send_message->status = EXECUTION_ERROR;
    }

    return;
}
