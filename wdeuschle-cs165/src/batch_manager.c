#include <stdbool.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>
#include "client_context.h"
#include "utils.h"

#define QUERIES_PER_THREAD 8
#define CHUNK_PROCESSING_SIZE ((int) 1 << 16)
#define MAX_THREAD_COUNT 4
#define DBO_CHUNK_SIZE 4


/*
 * add_shared_dbo receives a dbo to add to the batch of queries to be processed together
 */
void add_shared_dbo(DbOperator* dbo) {
    // check if we need to resize
    if (shared_scan_operators.num_dbos == shared_scan_operators.dbo_slots) {
        // get reference to old dbos and size
        DbOperator** old_dbos = shared_scan_operators.dbos;
        int old_size = shared_scan_operators.dbo_slots;
        // update the size
        shared_scan_operators.dbo_slots = 2 * old_size;
        // allocate more space, copy old data in
        shared_scan_operators.dbos = malloc(sizeof(DbOperator*) * shared_scan_operators.dbo_slots);
        memcpy(shared_scan_operators.dbos, old_dbos, (sizeof(DbOperator*) * old_size)); 
        // free old dbo references
        free(old_dbos);
    }

    // only doing selects for now
    // if we don't have the column being used yet, set it
    if (shared_scan_operators.col == NULL) {
        shared_scan_operators.col = dbo->operator_fields.select_operator.compare_info->gen_col.column_pointer.column;
        shared_scan_operators.num_entries = dbo->operator_fields.select_operator.num_results;
    }

    // add this dbo and increment the number of dbos we have
    shared_scan_operators.dbos[shared_scan_operators.num_dbos++] = dbo;

    return;
}

/*
 * construct_batch_operator composes all the previous queries submitted for
 * batch processing, returns the dbo to actually execute the scan, and 
 * cleans up the global shared_scan_operators
 */
DbOperator* construct_batch_operator() {
    // construct the dbo
    DbOperator* shared_dbo = malloc(sizeof(DbOperator));
    shared_dbo->type = SHARED_SCAN;
    // TODO:
    // prove this is more useful, or just move it into parse

    return shared_dbo;
}

bool validate_shared_scan() {
    // make sure every select operator is referring to the column we care about
    for (int i = 0; i < shared_scan_operators.num_dbos; ++i) {
        DbOperator* current_dbo = shared_scan_operators.dbos[i];
        if (current_dbo->operator_fields.select_operator.compare_info->gen_col.column_pointer.column != shared_scan_operators.col) {
            return false;
        }
    }

    return true;
}

// simple data structure to wrap a results array with its current count
typedef struct ResultAndCount {
    int* result;
    int count;
    struct DbOperator** dbos;
} ResultAndCount;

// simple data structure to wrap a results array with its current count
typedef struct ResultAndCountBitVector {
    int* result;
    int count;
    struct DbOperator** dbos;
} ResultAndCountBitVector;

typedef struct SharedScanThreadObj {
    struct ResultAndCount* results_and_counts;
    int num_queries; // how many queries the thread needs to execute
} SharedScanThreadObj;

struct TaskList;

// object used by a thread to perform a unit of work (this is a task)
typedef struct SharedScanThreadObjBitVector {
    ResultAndCountBitVector* results_and_counts;
    int num_queries; // how many queries the thread needs to execute
    int data_sz;
    int* data_start;
    int result_offset;
    struct TaskList* task_list_head;
} SharedScanThreadObjBitVector;

// list of tasks
typedef struct TaskList {
    bool is_head;
    struct TaskList* next;
    struct SharedScanThreadObjBitVector* sst_obj;
    pthread_mutex_t* lock;
} TaskList;

// information that execute_shared_scan_bitvector uses to clean up memory
// after executing the shared scan
typedef struct SharedScanToFree {
    struct TaskList* task_list_head;
    struct ResultAndCountBitVector* results_and_counts;
    struct SharedScanThreadObjBitVector* sst_objs;
} SharedScanToFree;

// push a task onto our queue, add it to the front
void task_list_push(TaskList* task_list_head, SharedScanThreadObjBitVector* sst_obj) {
    assert(task_list_head && task_list_head->is_head);

    // grab the lock
    pthread_mutex_lock(task_list_head->lock);

    // create a new TaskList item and add it to the end
    TaskList* new_task_list_item = malloc(sizeof(TaskList));
    new_task_list_item->next = NULL;
    new_task_list_item->sst_obj = sst_obj;
    new_task_list_item->is_head = false;
    new_task_list_item->lock = NULL;

    // assign to front of task list
    new_task_list_item->next = task_list_head->next;
    task_list_head->next = new_task_list_item;

    // unlock mutex
    pthread_mutex_unlock(task_list_head->lock);

    return;
}

// remove the next unit of work from our queue, get it from the back
SharedScanThreadObjBitVector* task_list_pop(TaskList* task_list_head) {
    assert(task_list_head && task_list_head->is_head);

    // grab the lock
    pthread_mutex_lock(task_list_head->lock);

    // find the end
    // done if this is the only node
    if (task_list_head->next == NULL) {
        // unlock mutex
        pthread_mutex_unlock(task_list_head->lock);
        return NULL;
    }
    TaskList* current_tl = task_list_head;
    TaskList* prev = NULL;
    while (current_tl->next) {
        prev = current_tl;
        current_tl = current_tl->next;
    }
    // chop off this node
    prev->next = NULL;
    // grab our return value
    SharedScanThreadObjBitVector* sst_obj = current_tl->sst_obj;
    // free the node
    free(current_tl);

    // unlock mutex
    pthread_mutex_unlock(task_list_head->lock);

    // return the sst_obj
    return sst_obj;
}

// free the task list head, clean up other things
void free_task_list_head(TaskList* task_list_head) {
    assert(task_list_head && task_list_head->is_head);
    // destroy the lock
    pthread_mutex_destroy(task_list_head->lock);
    free(task_list_head->lock);
    free(task_list_head);
    return;
}

// initialize the list of tasks
SharedScanToFree setup_task_list(void) {
    // ALLOCATE AND INITIALIZE OBJECTS FOR SHARED SCANS
    // to free: results_and_counts, sst_objs, task list

    int num_bitvector_ints = num_bitvector_ints_needed(shared_scan_operators.num_entries);
    log_info("how many bitvector_ints %d\n", num_bitvector_ints);
    // allocate results/counts array for each dbo
    ResultAndCountBitVector* results_and_counts = malloc(sizeof(ResultAndCountBitVector) * shared_scan_operators.num_dbos);
    for (int i = 0; i < shared_scan_operators.num_dbos; ++i) {
        results_and_counts[i].result = calloc(num_bitvector_ints, sizeof(int));
        results_and_counts[i].count = 0;
        results_and_counts[i].dbos = shared_scan_operators.dbos + i;
    }

    // calculate number of tasks needed: based on number of queries and size of data
    // #1: number of queries portion
    int num_query_chunks = shared_scan_operators.num_dbos / QUERIES_PER_THREAD;
    // add one more if not evenly split
    if (shared_scan_operators.num_dbos % QUERIES_PER_THREAD > 0) {
        ++num_query_chunks;
    }
    // #2: size of data portion
    int num_data_chunks = shared_scan_operators.num_entries / CHUNK_PROCESSING_SIZE;
    // add one more if not evenly split
    if (shared_scan_operators.num_entries % CHUNK_PROCESSING_SIZE > 0) {
        ++num_data_chunks;
    }

    int total_units_of_work = num_query_chunks * num_data_chunks;

    // allocate all the needed SharedScanThreadObjBitVector, which is an
    // individual unit of work for a thread
    SharedScanThreadObjBitVector* sst_objs = malloc(sizeof(SharedScanThreadObjBitVector) * total_units_of_work); 

    // create a lock for our task list
    pthread_mutex_t* lock = malloc(sizeof(pthread_mutex_t));
    // initialize it
    if (pthread_mutex_init(lock, NULL) != 0) {
        // TODO: error handle
        log_err("Failed to create mutex\n");
    }

    // setup these units of work in a task list
    TaskList* task_list_head = malloc(sizeof(TaskList));
    task_list_head->next = NULL;
    task_list_head->sst_obj = NULL;
    task_list_head->is_head = true;
    task_list_head->lock = lock;

    // organize work units
    // loops through our data one data chunk at a time
    for (int c = 0; c < num_data_chunks; ++c) {

        int data_offset = c * CHUNK_PROCESSING_SIZE;
        int result_offset = data_offset / BITS_PER_INT;
        int data_sz = CHUNK_PROCESSING_SIZE;
        if (shared_scan_operators.num_entries - data_offset < data_sz) {
            data_sz = shared_scan_operators.num_entries - data_offset;
        }
        int* data_start = shared_scan_operators.col->data + data_offset;

        // assign to a unit of work (SharedScanThreadObjBitVector)
        for (int i = 0; i < num_query_chunks; ++i) {
            // get start of ResultAndCount
            ResultAndCountBitVector* start_res_and_count = results_and_counts + (i * QUERIES_PER_THREAD);
            // create the thread obj
            int num_queries = QUERIES_PER_THREAD;
            // account for the last set of queries, which might not divide 
            // QUERIES_PER_THREAD evenly
            if (shared_scan_operators.num_dbos - (QUERIES_PER_THREAD * i) < QUERIES_PER_THREAD) {
                num_queries = shared_scan_operators.num_dbos - (QUERIES_PER_THREAD * i);
            }
            log_info("num_queries at creation time: %d\n", num_queries);
            SharedScanThreadObjBitVector* sst_obj = sst_objs + (c * num_query_chunks) + i;
            sst_obj->results_and_counts = start_res_and_count;
            sst_obj->num_queries = num_queries;
            sst_obj->data_sz = data_sz;
            sst_obj->data_start = data_start;
            sst_obj->result_offset = result_offset;
            sst_obj->task_list_head = task_list_head;

            // add to task list
            task_list_push(task_list_head, sst_obj);
        }
    }
    SharedScanToFree to_free = { task_list_head, results_and_counts, sst_objs };
    // for freeing from the caller
    return to_free;
}

/*
 * shared_scan_helper is a helper function that does the actual scanning
 */
void* shared_scan_helper(void* sst_obj_void) {
    SharedScanThreadObj* sst_obj = (SharedScanThreadObj*) sst_obj_void;
    log_info("USING THREADS, num_queries: %d\n", sst_obj->num_queries);
    // access result and count
    ResultAndCount* results_and_counts = sst_obj->results_and_counts;
    // iterate through all our data, perform the comparison at each step
    for (int i = 0; i < shared_scan_operators.num_entries; ++i) {
        // get current value from base data
        int current_val = shared_scan_operators.col->data[i];
        // iterate over each dbo
        for (int j = 0; j < sst_obj->num_queries; ++j) {
            Comparator* current_compare_info = results_and_counts->dbos[j]->operator_fields.select_operator.compare_info;
            // access the result/count for this query
            ResultAndCount* current_res_and_count = &results_and_counts[j];
            // if it passes the check, add it to the results
            if (current_val < current_compare_info->p_high && current_val >= current_compare_info->p_low) {
                // add it and increment the count
                current_res_and_count->result[current_res_and_count->count++] = i;
            }
        }
    }
    return NULL;
}

void* shared_scan_helper_bitvector(SharedScanThreadObjBitVector* sst_obj) {
    log_info("USING THREADS, num_queries: %d\n", sst_obj->num_queries);
    // access result and count
    ResultAndCountBitVector* results_and_counts = sst_obj->results_and_counts;

    // OPTIMIZATION: THIS WAS WAY SLOWER
    // iterate through all our data, perform the comparison at each step
    /*for (int i = 0; i < sst_obj->data_sz; ++i) {*/
        /*// get current value from base data*/
        /*int current_val = sst_obj->data_start[i];*/
        /*// iterate over each dbo*/
        /*for (int j = 0; j < sst_obj->num_queries; ++j) {*/
            /*Comparator* current_compare_info = results_and_counts->dbos[j]->operator_fields.select_operator.compare_info;*/
            /*// access the result/count for this query*/
            /*ResultAndCountBitVector* current_res_and_count = &results_and_counts[j];*/
            /*int* result_start = current_res_and_count->result + sst_obj->result_offset;*/
            /*// if it passes the check, add it to the results*/
            /*if (current_val < current_compare_info->p_high && current_val >= current_compare_info->p_low) {*/
                /*// update the bitvector*/
                /*// get the right char*/
                /*int int_idx = i / BITS_PER_INT;*/
                /*int int_offset = i % BITS_PER_INT;*/
                /*// we have a pointer to the start of the result bitvector that we're updating*/
                /*result_start[int_idx] = (result_start[int_idx] | (1 << int_offset));*/
                /*// increment the count*/
                /*++current_res_and_count->count;*/
            /*}*/
        /*}*/
    /*}*/

    // this looped version is slower for some reason W/O optimization
    // NOTE: this is the same thing as the unrolled by hand version
    // when we run with an optimized build - likely gets unrolled
    /*for (int j = 0; j < sst_obj->num_queries;) {*/
        /*if (j - 1 + DBO_CHUNK_SIZE < sst_obj->num_queries) {*/
            /*Comparator* current_compare_infos[DBO_CHUNK_SIZE];*/
            /*ResultAndCountBitVector* current_res_and_counts[DBO_CHUNK_SIZE];*/
            /*int* result_starts[DBO_CHUNK_SIZE];*/
            /*for (int i = 0; i < DBO_CHUNK_SIZE; ++i) {*/
                /*current_compare_infos[i] = results_and_counts->dbos[j + i]->operator_fields.select_operator.compare_info;*/
                /*current_res_and_counts[i] = &results_and_counts[j + i];*/
                /*result_starts[i] = current_res_and_counts[i]->result + sst_obj->result_offset;*/
            /*}*/
            /*// iterate through all our data, perform the comparison at each step*/
            /*// adding a zig zag pattern is slightly faster*/
            /*if (j % 2 == 0) {*/
                /*for (int i = 0; i < sst_obj->data_sz; ++i) {*/
                    /*// get current value from base data*/
                    /*int current_val = sst_obj->data_start[i];*/
                    /*// if it passes the check, add it to the results*/
                    /*for (int k = 0; k < DBO_CHUNK_SIZE; ++k) {*/
                        /*if (current_val < current_compare_infos[k]->p_high && current_val >= current_compare_infos[k]->p_low) {*/
                            /*// update the bitvector*/
                            /*// get the right char*/
                            /*int int_idx = i / BITS_PER_INT;*/
                            /*int int_offset = i % BITS_PER_INT;*/
                            /*// we have a pointer to the start of the result bitvector that we're updating*/
                            /*result_starts[k][int_idx] = (result_starts[k][int_idx] | (1 << int_offset));*/
                            /*// increment the count*/
                            /*++current_res_and_counts[k]->count;*/
                        /*}*/
                    /*}*/
                /*}*/
            /*} else {*/
                /*for (int i = sst_obj->data_sz - 1; i >= 0; --i) {*/
                    /*// get current value from base data*/
                    /*int current_val = sst_obj->data_start[i];*/
                    /*// if it passes the check, add it to the results*/
                    /*for (int k = 0; k < DBO_CHUNK_SIZE; ++k) {*/
                        /*if (current_val < current_compare_infos[k]->p_high && current_val >= current_compare_infos[k]->p_low) {*/
                            /*// update the bitvector*/
                            /*// get the right char*/
                            /*int int_idx = i / BITS_PER_INT;*/
                            /*int int_offset = i % BITS_PER_INT;*/
                            /*// we have a pointer to the start of the result bitvector that we're updating*/
                            /*result_starts[k][int_idx] = (result_starts[k][int_idx] | (1 << int_offset));*/
                            /*// increment the count*/
                            /*++current_res_and_counts[k]->count;*/
                        /*}*/
                    /*}*/
                /*}*/
            /*}*/
            /*// increment j*/
            /*j += DBO_CHUNK_SIZE;*/
        /*} else {*/
            /*Comparator* current_compare_info = results_and_counts->dbos[j]->operator_fields.select_operator.compare_info;*/
            /*// access the result/count for this query*/
            /*ResultAndCountBitVector* current_res_and_count = &results_and_counts[j];*/
            /*int* result_start = current_res_and_count->result + sst_obj->result_offset;*/
            /*// iterate through all our data, perform the comparison at each step*/
            /*// adding a zig zag pattern is slightly faster*/
            /*if (j % 2 == 0) {*/
                /*for (int i = 0; i < sst_obj->data_sz; ++i) {*/
                    /*// get current value from base data*/
                    /*int current_val = sst_obj->data_start[i];*/
                    /*// if it passes the check, add it to the results*/
                    /*if (current_val < current_compare_info->p_high && current_val >= current_compare_info->p_low) {*/
                        /*// update the bitvector*/
                        /*// get the right char*/
                        /*int int_idx = i / BITS_PER_INT;*/
                        /*int int_offset = i % BITS_PER_INT;*/
                        /*// we have a pointer to the start of the result bitvector that we're updating*/
                        /*result_start[int_idx] = (result_start[int_idx] | (1 << int_offset));*/
                        /*// increment the count*/
                        /*++current_res_and_count->count;*/
                    /*}*/
                /*}*/
            /*} else {*/
                /*for (int i = sst_obj->data_sz - 1; i >= 0; --i) {*/
                    /*// get current value from base data*/
                    /*int current_val = sst_obj->data_start[i];*/
                    /*// if it passes the check, add it to the results*/
                    /*if (current_val < current_compare_info->p_high && current_val >= current_compare_info->p_low) {*/
                        /*// update the bitvector*/
                        /*// get the right char*/
                        /*int int_idx = i / BITS_PER_INT;*/
                        /*int int_offset = i % BITS_PER_INT;*/
                        /*// we have a pointer to the start of the result bitvector that we're updating*/
                        /*result_start[int_idx] = (result_start[int_idx] | (1 << int_offset));*/
                        /*// increment the count*/
                        /*++current_res_and_count->count;*/
                    /*}*/
                /*}*/
            /*}*/
            /*// increment j*/
            /*j += 1;*/
        /*}*/
    /*}*/

    // OPTIMIZATION: FASTER, LOOPING OVER THE DBOS IN THE OUTSIDE LOOP
    // (ALSO USING UNROLLING) AND LOOPING OVER DATA INSIDE
    // THIS HAS BY HAND UNROLLING, WHEN OPTIMIZED ITS THE SAME
    // iterate over each dbo
    for (int j = 0; j < sst_obj->num_queries;) {
        if (j - 1 + DBO_CHUNK_SIZE < sst_obj->num_queries) {
            // loop unrolling
            Comparator* current_compare_info1 = results_and_counts->dbos[j]->operator_fields.select_operator.compare_info;
            Comparator* current_compare_info2 = results_and_counts->dbos[j + 1]->operator_fields.select_operator.compare_info;
            Comparator* current_compare_info3 = results_and_counts->dbos[j + 2]->operator_fields.select_operator.compare_info;
            Comparator* current_compare_info4 = results_and_counts->dbos[j + 3]->operator_fields.select_operator.compare_info;
            // access the result/count for these queries
            ResultAndCountBitVector* current_res_and_count1 = &results_and_counts[j];
            ResultAndCountBitVector* current_res_and_count2 = &results_and_counts[j + 1];
            ResultAndCountBitVector* current_res_and_count3 = &results_and_counts[j + 2];
            ResultAndCountBitVector* current_res_and_count4 = &results_and_counts[j + 3];
            int* result_start1 = current_res_and_count1->result + sst_obj->result_offset;
            int* result_start2 = current_res_and_count2->result + sst_obj->result_offset;
            int* result_start3 = current_res_and_count3->result + sst_obj->result_offset;
            int* result_start4 = current_res_and_count4->result + sst_obj->result_offset;
            // iterate through all our data, perform the comparison at each step
            // adding a zig zag pattern is slightly faster
            if (j % 2 == 0) {
                for (int i = 0; i < sst_obj->data_sz; ++i) {
                    // get current value from base data
                    int current_val = sst_obj->data_start[i];
                    // if it passes the check, add it to the results
                    if (current_val < current_compare_info1->p_high && current_val >= current_compare_info1->p_low) {
                        // update the bitvector
                        // get the right char
                        int int_idx = i / BITS_PER_INT;
                        int int_offset = i % BITS_PER_INT;
                        // we have a pointer to the start of the result bitvector that we're updating
                        result_start1[int_idx] = (result_start1[int_idx] | (1 << int_offset));
                        // increment the count
                        ++current_res_and_count1->count;
                    }
                    if (current_val < current_compare_info2->p_high && current_val >= current_compare_info2->p_low) {
                        // update the bitvector
                        // get the right char
                        int int_idx = i / BITS_PER_INT;
                        int int_offset = i % BITS_PER_INT;
                        // we have a pointer to the start of the result bitvector that we're updating
                        result_start2[int_idx] = (result_start2[int_idx] | (1 << int_offset));
                        // increment the count
                        ++current_res_and_count2->count;
                    }
                    if (current_val < current_compare_info3->p_high && current_val >= current_compare_info3->p_low) {
                        // update the bitvector
                        // get the right char
                        int int_idx = i / BITS_PER_INT;
                        int int_offset = i % BITS_PER_INT;
                        // we have a pointer to the start of the result bitvector that we're updating
                        result_start3[int_idx] = (result_start3[int_idx] | (1 << int_offset));
                        // increment the count
                        ++current_res_and_count3->count;
                    }
                    if (current_val < current_compare_info4->p_high && current_val >= current_compare_info4->p_low) {
                        // update the bitvector
                        // get the right char
                        int int_idx = i / BITS_PER_INT;
                        int int_offset = i % BITS_PER_INT;
                        // we have a pointer to the start of the result bitvector that we're updating
                        result_start4[int_idx] = (result_start4[int_idx] | (1 << int_offset));
                        // increment the count
                        ++current_res_and_count4->count;
                    }
                }
            } else {
                for (int i = sst_obj->data_sz - 1; i >= 0; --i) {
                    // get current value from base data
                    int current_val = sst_obj->data_start[i];
                    // if it passes the check, add it to the results
                    if (current_val < current_compare_info1->p_high && current_val >= current_compare_info1->p_low) {
                        // update the bitvector
                        // get the right char
                        int int_idx = i / BITS_PER_INT;
                        int int_offset = i % BITS_PER_INT;
                        // we have a pointer to the start of the result bitvector that we're updating
                        result_start1[int_idx] = (result_start1[int_idx] | (1 << int_offset));
                        // increment the count
                        ++current_res_and_count1->count;
                    }
                    if (current_val < current_compare_info2->p_high && current_val >= current_compare_info2->p_low) {
                        // update the bitvector
                        // get the right char
                        int int_idx = i / BITS_PER_INT;
                        int int_offset = i % BITS_PER_INT;
                        // we have a pointer to the start of the result bitvector that we're updating
                        result_start2[int_idx] = (result_start2[int_idx] | (1 << int_offset));
                        // increment the count
                        ++current_res_and_count2->count;
                    }
                    if (current_val < current_compare_info3->p_high && current_val >= current_compare_info3->p_low) {
                        // update the bitvector
                        // get the right char
                        int int_idx = i / BITS_PER_INT;
                        int int_offset = i % BITS_PER_INT;
                        // we have a pointer to the start of the result bitvector that we're updating
                        result_start3[int_idx] = (result_start3[int_idx] | (1 << int_offset));
                        // increment the count
                        ++current_res_and_count3->count;
                    }
                    if (current_val < current_compare_info4->p_high && current_val >= current_compare_info4->p_low) {
                        // update the bitvector
                        // get the right char
                        int int_idx = i / BITS_PER_INT;
                        int int_offset = i % BITS_PER_INT;
                        // we have a pointer to the start of the result bitvector that we're updating
                        result_start4[int_idx] = (result_start4[int_idx] | (1 << int_offset));
                        // increment the count
                        ++current_res_and_count4->count;
                    }
                }
            }
            // increment j
            j += DBO_CHUNK_SIZE;
        } else {
            Comparator* current_compare_info = results_and_counts->dbos[j]->operator_fields.select_operator.compare_info;
            // access the result/count for this query
            ResultAndCountBitVector* current_res_and_count = &results_and_counts[j];
            int* result_start = current_res_and_count->result + sst_obj->result_offset;
            // iterate through all our data, perform the comparison at each step
            // adding a zig zag pattern is slightly faster
            if (j % 2 == 0) {
                for (int i = 0; i < sst_obj->data_sz; ++i) {
                    // get current value from base data
                    int current_val = sst_obj->data_start[i];
                    // if it passes the check, add it to the results
                    if (current_val < current_compare_info->p_high && current_val >= current_compare_info->p_low) {
                        // update the bitvector
                        // get the right char
                        int int_idx = i / BITS_PER_INT;
                        int int_offset = i % BITS_PER_INT;
                        // we have a pointer to the start of the result bitvector that we're updating
                        result_start[int_idx] = (result_start[int_idx] | (1 << int_offset));
                        // increment the count
                        ++current_res_and_count->count;
                    }
                }
            } else {
                for (int i = sst_obj->data_sz - 1; i >= 0; --i) {
                    // get current value from base data
                    int current_val = sst_obj->data_start[i];
                    // if it passes the check, add it to the results
                    if (current_val < current_compare_info->p_high && current_val >= current_compare_info->p_low) {
                        // update the bitvector
                        // get the right char
                        int int_idx = i / BITS_PER_INT;
                        int int_offset = i % BITS_PER_INT;
                        // we have a pointer to the start of the result bitvector that we're updating
                        result_start[int_idx] = (result_start[int_idx] | (1 << int_offset));
                        // increment the count
                        ++current_res_and_count->count;
                    }
                }
            }
            // increment j
            j += 1;
        }
    }
    return NULL;
}

/*
 * execute_shared_scan receives the shared dbo, coordinates their execution,
 * and returns a result message
 */
void execute_shared_scan_columnwise(DbOperator* shared_dbo, message* send_message) {
    log_info("executing shared scan columnwise\n");

    if (validate_shared_scan() == false) {
        const char* result_message = "shared scan invalid";
        char* result_message_ptr = malloc(strlen(result_message) + 1);
        strcpy(result_message_ptr, result_message);
        send_message->payload = result_message_ptr;
        send_message->status = OK_DONE;
        return;
    }

    log_info("number of scans to execute: %d\n", shared_scan_operators.num_dbos);

    // ALLOCATE AND INITIALIZE OBJECTS FOR SHARED SCANS
    // to free: results_and_counts, threads, sst_objs

    // allocate results/counts array for each dbo
    ResultAndCount* results_and_counts = malloc(sizeof(ResultAndCount) * shared_scan_operators.num_dbos);
    for (int i = 0; i < shared_scan_operators.num_dbos; ++i) {
        // TODO: resize results somehow?
        results_and_counts[i].result = malloc(shared_scan_operators.num_entries * sizeof(int));
        results_and_counts[i].count = 0;
        results_and_counts[i].dbos = shared_scan_operators.dbos + i;
    }

    // calculate number of threads needed
    int num_threads = shared_scan_operators.num_dbos / QUERIES_PER_THREAD;
    // add one more if not evenly split
    if (shared_scan_operators.num_dbos % QUERIES_PER_THREAD > 0) {
        ++num_threads;
    }
    // allocate threads
    pthread_t* threads = malloc(sizeof(pthread_t) * num_threads);
    // allocate thread objs
    SharedScanThreadObj* sst_objs = malloc(sizeof(SharedScanThreadObj) * num_threads); 

    // assign to threads
    for (int i = 0; i < num_threads; ++i) {
        // get start of ResultAndCount
        ResultAndCount* start_res_and_count = results_and_counts + (i * QUERIES_PER_THREAD);
        // create the thread obj
        int num_queries = QUERIES_PER_THREAD;
        // set to one if there's only one left
        if (shared_scan_operators.num_dbos - (QUERIES_PER_THREAD * i) < QUERIES_PER_THREAD) {
            num_queries = shared_scan_operators.num_dbos - (QUERIES_PER_THREAD * i);
        }
        log_info("num_queries at creation time: %d\n", num_queries);
        SharedScanThreadObj* sst_obj = sst_objs + i;
        sst_obj->results_and_counts = start_res_and_count;
        sst_obj->num_queries = num_queries;

        // assign to thread
        if (pthread_create(&threads[i], NULL, shared_scan_helper, sst_obj)) {
            const char* result_message = "creating thread failed";
            char* result_message_ptr = malloc(strlen(result_message) + 1);
            strcpy(result_message_ptr, result_message);
            send_message->payload = result_message_ptr;
            send_message->status = OK_DONE;
            // free objects from shared scans
            free(results_and_counts);
            free(threads);
            free(sst_objs);
            return;
        }
    }

    // wait for all our threads to finish
    for (int i = 0; i < num_threads; ++i) {
        if (pthread_join(threads[i], NULL)) {
            const char* result_message = "thread failed to execute";
            char* result_message_ptr = malloc(strlen(result_message) + 1);
            strcpy(result_message_ptr, result_message);
            send_message->payload = result_message_ptr;
            send_message->status = OK_DONE;
            // free objects from shared scans
            free(results_and_counts);
            free(threads);
            free(sst_objs);
            return;
        }
    }

    // once it's done, save the results
    for (int i = 0; i < shared_scan_operators.num_dbos; ++i) {
        // access the current result/count and dbo
        ResultAndCount current_res_and_count = results_and_counts[i];
        DbOperator* current_dbo = shared_scan_operators.dbos[i];

        // create result obj for each result
        Result* result_obj = malloc(sizeof(Result));
        result_obj->num_tuples = current_res_and_count.count;
        result_obj->payload = current_res_and_count.result;
        result_obj->data_type = INT;
        result_obj->is_posn_vector = true;

        // add each result to the client
        GeneralizedColumnHandle generalized_result_handle;
        // add name to result wrapper
        strcpy(generalized_result_handle.name, current_dbo->operator_fields.select_operator.compare_info->handle);
        // update type
        generalized_result_handle.generalized_column.column_type = RESULT;
        // add the result
        generalized_result_handle.generalized_column.column_pointer.result = result_obj;

        // add this value to the client context variable pool
        add_to_client_context(shared_dbo->context, generalized_result_handle);
    }
    
    // free objects from shared scans
    free(results_and_counts);
    free(threads);
    free(sst_objs);

    // now that we've finished executing the results, clean up the shared_scan_operators
    shared_scan_operators.num_dbos = 0;
    shared_scan_operators.col = NULL;
    shared_scan_operators.num_entries = 0;
    
    const char* result_message = "shared scan successful";
    char* result_message_ptr = malloc(strlen(result_message) + 1);
    strcpy(result_message_ptr, result_message);
    send_message->payload = result_message_ptr;
    send_message->status = OK_DONE;
    return;
}

// thread performs work through this mechanism
void* do_task(void* void_task) {
    // the task
    SharedScanThreadObjBitVector* task = (SharedScanThreadObjBitVector*) void_task;
    while (task) {
        // timing
        // ******************
        clock_t start, end;
        double cpu_time_used;
        start = clock();
        // ******************
        // perform the scan
        shared_scan_helper_bitvector(task);
        // timing
        // ******************
        end = clock();
        cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
        log_timing("time to DO TASK: %f\n", cpu_time_used);
        // ******************
        // get the next task
        task = task_list_pop(task->task_list_head);
    }
    // done
    return NULL;
}

void execute_shared_scan_bitvector(DbOperator* shared_dbo, message* send_message) {
    log_info("executing shared scan bitvector\n");

    if (validate_shared_scan() == false) {
        const char* result_message = "shared scan invalid";
        char* result_message_ptr = malloc(strlen(result_message) + 1);
        strcpy(result_message_ptr, result_message);
        send_message->payload = result_message_ptr;
        send_message->status = OK_DONE;
        return;
    }

    log_info("number of scans to execute: %d\n", shared_scan_operators.num_dbos);

    // set up our tasks
    SharedScanToFree to_free = setup_task_list();

    // set up our threads
    pthread_t threads[MAX_THREAD_COUNT];

    // run our threads over our tasks
    for (int i = 0; i < MAX_THREAD_COUNT; ++i) {
        // assign to thread
        if (pthread_create(threads + i, NULL, do_task, task_list_pop(to_free.task_list_head))) {
            const char* result_message = "creating thread failed";
            char* result_message_ptr = malloc(strlen(result_message) + 1);
            strcpy(result_message_ptr, result_message);
            send_message->payload = result_message_ptr;
            send_message->status = OK_DONE;
            // free objects from shared scans
            free(to_free.results_and_counts);
            free(to_free.sst_objs);
            free_task_list_head(to_free.task_list_head);
            return;
        }
    }

    // join the threads
    for (int i = 0; i < MAX_THREAD_COUNT; ++i) {
        if (pthread_join(threads[i], NULL)) {
            const char* result_message = "thread failed to execute";
            char* result_message_ptr = malloc(strlen(result_message) + 1);
            strcpy(result_message_ptr, result_message);
            send_message->payload = result_message_ptr;
            send_message->status = OK_DONE;
            // free objects from shared scans
            free(to_free.results_and_counts);
            free(to_free.sst_objs);
            free_task_list_head(to_free.task_list_head);
            return;
        }
    }

    // need to know how many bitvector ints were used
    int num_bitvector_ints = num_bitvector_ints_needed(shared_scan_operators.num_entries);
    // once it's done, save the results
    for (int i = 0; i < shared_scan_operators.num_dbos; ++i) {
        // access the current result/count and dbo
        ResultAndCountBitVector current_res_and_count = to_free.results_and_counts[i];
        DbOperator* current_dbo = shared_scan_operators.dbos[i];

        // create result obj for each result
        Result* result_obj = malloc(sizeof(Result));
        result_obj->num_tuples = current_res_and_count.count;
        result_obj->payload = current_res_and_count.result;
        result_obj->data_type = INT;
        result_obj->bitvector_ints = num_bitvector_ints;
        result_obj->is_posn_vector = false; // this is a bitvector

        // add each result to the client
        GeneralizedColumnHandle generalized_result_handle;
        // add name to result wrapper
        strcpy(generalized_result_handle.name, current_dbo->operator_fields.select_operator.compare_info->handle);
        // update type
        generalized_result_handle.generalized_column.column_type = RESULT;
        // add the result
        generalized_result_handle.generalized_column.column_pointer.result = result_obj;

        // add this value to the client context variable pool
        add_to_client_context(shared_dbo->context, generalized_result_handle);
    }
    
    // free objects from shared scans
    free(to_free.results_and_counts);
    free(to_free.sst_objs);
    free_task_list_head(to_free.task_list_head);

    // now that we've finished executing the results, clean up the shared_scan_operators
    // free each of the dbos
    for (int i = 0; i < shared_scan_operators.num_dbos; ++i) {
        db_operator_free(shared_scan_operators.dbos[i]);
    }
    free(shared_scan_operators.dbos);
    shared_scan_operators.num_dbos = 0;
    shared_scan_operators.col = NULL;
    shared_scan_operators.num_entries = 0;
    
    const char* result_message = "shared scan successful";
    char* result_message_ptr = malloc(strlen(result_message) + 1);
    strcpy(result_message_ptr, result_message);
    send_message->payload = result_message_ptr;
    send_message->status = OK_DONE;
    return;
}

/* 
 * this function dispatches the correct shared scan function based on global flags
 */
void execute_shared_scan(DbOperator* shared_dbo, message* send_message) {
    if (BITVECTOR_DB) {
        execute_shared_scan_bitvector(shared_dbo, send_message);
    } else {
        execute_shared_scan_columnwise(shared_dbo, send_message);
    }
}
