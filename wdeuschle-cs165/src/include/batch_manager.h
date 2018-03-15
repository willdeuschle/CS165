#ifndef BATCH_MANAGER_H
#define BATCH_MANAGER_H

#include <stdbool.h>
#include "cs165_api.h"



/*
 * add_shared_dbo receives a dbo to add to the batch of queries to be processed together
 */
void add_shared_dbo(DbOperator* dbo);

/*
 * construct_batch_operator composes all the previous queries submitted for
 * batch processing, returns the dbo to actually execute the scan, and 
 * cleans up the global shared_scan_operators
 */
DbOperator* construct_batch_operator();

/*
 * validate_shared_scan runs checks to make sure that the queries
 * we are trying to execute are actually eligible for a shared scan
 */
bool validate_shared_scan();

/*
 * shared_scan_helper is a helper function that does the actual scanning
 */
void* shared_scan_helper(void* sst_obj_void);

/*
 * execute_shared_scan receives the shared dbo, coordinates their execution
 * using columns, and returns a result message
 */
void execute_shared_scan_columnwise(DbOperator* shared_dbo, message* send_message);

/*
 * execute_shared_scan receives the shared dbo, coordinates their execution 
 * using bitvectors and returns a result message
 */
void execute_shared_scan_bitvector(DbOperator* shared_dbo, message* send_message);

/* 
 * this function dispatches the correct shared scan function based on global flags
 */
void execute_shared_scan(DbOperator* shared_dbo, message* send_message);

#endif
