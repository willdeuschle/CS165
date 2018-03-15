#ifndef DB_READS_H
#define DB_READS_H

#include "cs165_api.h"

/* 
 * this function selects information from our database and stores the result
 * as a bitvector
 * it returns a status string
 */
void db_select_bitvector(DbOperator* query, message* send_message);

/* 
 * this function selects information from our database and stores the result
 * as an array of ids
 * it returns a status string
 */
void db_select_columnwise(DbOperator* query, message* send_message);

/* 
 * this function dispatches the correct select function based on global flags
 */
void db_select(DbOperator* query, message* send_message);

/* 
 * this aggregrates the results of a select operation using bitvectors
 */
void db_fetch_bitvector(DbOperator* query, message* send_message);

/* 
 * this function dispatches the correct fetch function based on global flags
 */
void db_fetch(DbOperator* query, message* send_message);

/* 
 * this aggregrates the results of a select operation using columns
 */
void db_fetch(DbOperator* query, message* send_message);

/* 
 * this averages the results in a column
 */
void db_average(DbOperator* query, message* send_message);

/* 
 * this sums the results in a column
 */
void db_sum(DbOperator* query, message* send_message);

/* 
 * this find the min in a column
 */
void db_min(DbOperator* query, message* send_message);

/* 
 * this find the max in a column
 */
void db_max(DbOperator* query, message* send_message);

/* 
 * this adds the results in two columns
 */
void db_add(DbOperator* query, message* send_message, int mult_factor);

/* 
 * this function serializes and returns the results of some previous operation
 * to the client
 */
void serialized_db_print(DbOperator* query, message* send_message);

/* 
 * this function sends data to the client, usually in the form of binary which
 * the client can then interpret. if we are mixing data types in the response,
 * we will serialize it on the server before sending as ascii to the client
 */
void db_print(DbOperator* query, message* send_message);

#endif /* DB_READS_H */
