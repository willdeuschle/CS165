// utils.h
// CS165 Fall 2015
//
// Provides utility and helper functions that may be useful throughout.
// Includes debugging tools.

#ifndef __UTILS_H__
#define __UTILS_H__

#include <stdarg.h>
#include <stdio.h>
#include "message.h"
#include "cs165_api.h"

#define BITS_PER_INT (sizeof(int) * 8)

/*
 * This function takes a DbOperator pointer, and does the necessary memory
 * cleanup.
 */
void db_operator_free(DbOperator* dbo);

/**
 * Convert a position bitvector to a normal vector of positions
 **/
int num_bitvector_ints_needed(int num_entries);

/**
 * Convert a position bitvector to a normal vector of positions
 **/
int* bitvector_to_vector(int* bv, int num_bv_ints, int num_entries);

/**
 * Computes the number of batches needed to send an entire message server to client.
 **/
int number_of_batches(int message_size);

/**
 * Takes a pointer to a string.
 * This method returns the database name string. The original string now
 * points to the table name.
 * This method destroys its input.
 **/
char* split_on_period(char** str, message_status* status);

/**
 * Takes a pointer to a string.
 * This method returns the original string truncated to where its first comma lies.
 * In addition, the original string now points to the first character after that comma.
 * This method destroys its input.
 **/
char* next_token(char** tokenizer, message_status* status);

/*
 * This is a borrowed function for converting from integers to strings since
 * the C standard doesn't support this conversion.
 */
char* itoa(long val, int base);

/**
 * trims newline characters from a string (in place)
 **/

char* trim_newline(char *str);

/**
 * trims parenthesis characters from a string (in place)
 **/

char* trim_parenthesis(char *str);

/**
 * trims whitespace characters from a string (in place)
 **/

char* trim_whitespace(char *str);

/**
 * trims quotations characters from a string (in place)
 **/

char* trim_quotes(char *str);

// cs165_log(out, format, ...)
// Writes the string from @format to the @out pointer, extendable for
// additional parameters.
//
// Usage: cs165_log(stderr, "%s: error at line: %d", __func__, __LINE__);
void cs165_log(FILE* out, const char *format, ...);

// log_err(format, ...)
// Writes the string from @format to stderr, extendable for
// additional parameters. Like cs165_log, but specifically to stderr.
//
// Usage: log_err("%s: error at line: %d", __func__, __LINE__);
void log_err(const char *format, ...);

// log_timing(format, ...)
// Writes the string from @format to stderr, extendable for
// additional parameters. Like cs165_log, but specifically to stderr.
//
// Usage: log_timing("%s: error at line: %d", __func__, __LINE__);
void log_timing(const char *format, ...);

// log_info(format, ...)
// Writes the string from @format to stdout, extendable for
// additional parameters. Like cs165_log, but specifically to stdout.
// Only use this when appropriate (e.g., denoting a specific checkpoint),
// else defer to using printf.
//
// Usage: log_info("Command received: %s", command_string);
void log_info(const char *format, ...);

#endif /* __UTILS_H__ */
