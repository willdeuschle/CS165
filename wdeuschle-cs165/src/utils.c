#define _BSD_SOURCE
#include <stdio.h>
#include <string.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
#include "utils.h"
#include "message.h"

#define ANSI_COLOR_RED     "\x1b[31m"
#define ANSI_COLOR_GREEN   "\x1b[32m"
#define ANSI_COLOR_RESET   "\x1b[0m"

#define LOG 1
/*#define LOG_ERR 1*/
/*#define LOG_INFO 1*/
/*#define LOG_TIMING 1*/
// send this many bytes server -> client at a time
#define SOCKET_TRANSMISSION_SIZE 4096

/*
 * This function takes a DbOperator pointer, and does the necessary memory
 * cleanup.
 */
void db_operator_free(DbOperator* dbo) {
    // don't do anything if this is a null pointer
    if (dbo == NULL) {
        return;
    }
    // free any information specific to this type of database operation
    if (dbo->type == SELECT) {
        // free the comparator
        free(dbo->operator_fields.select_operator.compare_info);
    } else if (dbo->type == INSERT) {
        // free the values pointer
        free(dbo->operator_fields.insert_operator.values);
    } else if (dbo->type == PRINT) {
        // free each of the result pointer
        free(dbo->operator_fields.print_operator.results);
    }
    // free the db operator itself
    free(dbo);
    return;
}

/**
 * Convert a position bitvector to a normal vector of positions
 **/
int num_bitvector_ints_needed(int num_entries) {
    return (num_entries / BITS_PER_INT) + (num_entries % BITS_PER_INT);
}

/**
 * Convert a position bitvector to a normal vector of positions
 **/
int* bitvector_to_vector(int* bv, int num_bv_ints, int num_entries) {
    // add padding (num_entries + 1) so that we can have a loop without branching
    int* results = malloc(sizeof(int) * (num_entries + 1));
    int num_results = 0;
    for (int i = 0; i < num_bv_ints; ++i) {
        int int_offset = 0;
        unsigned int current_int = bv[i];
        while (current_int > 0) {
            results[num_results] = (i * BITS_PER_INT) + int_offset;
            num_results += current_int & 1;
            current_int = current_int >> 1;
            ++int_offset;
        }
    }
    return results;
}

/**
 * Computes the number of batches needed to send an entire message server to client.
 **/
int number_of_batches(int message_size) {
    // compute number of batches we will need
    return (message_size / SOCKET_TRANSMISSION_SIZE) + (message_size % SOCKET_TRANSMISSION_SIZE > 0);
}

/**
 * Takes a pointer to a string.
 * This method returns the database name string. The original string now
 * points to the table name.
 * This method destroys its input.
 **/
char* split_on_period(char** str, message_status* status) {
    char* first_half = strsep(str, ".");
    if (first_half == NULL) {
        *status= INCORRECT_FORMAT;
    }
    return first_half;
}

/**
 * Takes a pointer to a string.
 * This method returns the original string truncated to where its first comma lies.
 * In addition, the original string now points to the first character after that comma.
 * This method destroys its input.
 **/
char* next_token(char** tokenizer, message_status* status) {
    char* token = strsep(tokenizer, ",");
    if (token == NULL) {
        *status= INCORRECT_FORMAT;
    }
    return token;
}

/*
 * This is a borrowed function for converting from integers to strings since
 * the C standard doesn't support this conversion.
 */
char* itoa(long val, int base){
	static char buf[32] = {0};
	int i = 30;
    // handle case of 0
    if (val == 0) {
        buf[i] = '0';
        --i;
    } else {
        for(; val && i ; --i, val /= base)
            buf[i] = "0123456789abcdef"[val % base];
    }
	return &buf[i+1];
}


/* removes newline characters from the input string.
 * Shifts characters over and shortens the length of
 * the string by the number of newline characters.
 */ 
char* trim_newline(char *str) {
    int length = strlen(str);
    int current = 0;
    for (int i = 0; i < length; ++i) {
        if (!(str[i] == '\r' || str[i] == '\n')) {
            str[current++] = str[i];
        }
    }

    // Write new null terminator
    str[current] = '\0';
    return str;
}
/* removes space characters from the input string.
 * Shifts characters over and shortens the length of
 * the string by the number of space characters.
 */ 
char* trim_whitespace(char *str)
{
    int length = strlen(str);
    int current = 0;
    for (int i = 0; i < length; ++i) {
        if (!isspace(str[i])) {
            str[current++] = str[i];
        }
    }

    // Write new null terminator
    str[current] = '\0';
    return str;
}

/* removes parenthesis characters from the input string.
 * Shifts characters over and shortens the length of
 * the string by the number of parenthesis characters.
 */ 
char* trim_parenthesis(char *str) {
    int length = strlen(str);
    int current = 0;
    for (int i = 0; i < length; ++i) {
        if (!(str[i] == '(' || str[i] == ')')) {
            str[current++] = str[i];
        }
    }

    // Write new null terminator
    str[current] = '\0';
    return str;
}

char* trim_quotes(char *str) {
    int length = strlen(str);
    int current = 0;
    for (int i = 0; i < length; ++i) {
        if (str[i] != '\"') {
            str[current++] = str[i];
        }
    }

    // Write new null terminator
    str[current] = '\0';
    return str;
}
/* The following three functions will show output on the terminal
 * based off whether the corresponding level is defined.
 * To see log output, define LOG.
 * To see error output, define LOG_ERR.
 * To see info output, define LOG_INFO
 */
void cs165_log(FILE* out, const char *format, ...) {
#ifdef LOG
    va_list v;
    va_start(v, format);
    vfprintf(out, format, v);
    va_end(v);
#else
    (void) out;
    (void) format;
#endif
}

void log_err(const char *format, ...) {
#ifdef LOG_ERR
    va_list v;
    va_start(v, format);
    fprintf(stderr, ANSI_COLOR_RED);
    vfprintf(stderr, format, v);
    fprintf(stderr, ANSI_COLOR_RESET);
    va_end(v);
#else
    (void) format;
#endif
}
void log_timing(const char *format, ...) {
#ifdef LOG_TIMING
    va_list v;
    va_start(v, format);
    fprintf(stderr, ANSI_COLOR_RED);
    vfprintf(stderr, format, v);
    fprintf(stderr, ANSI_COLOR_RESET);
    va_end(v);
#else
    (void) format;
#endif
}

void log_info(const char *format, ...) {
#ifdef LOG_INFO
    va_list v;
    va_start(v, format);
    fprintf(stdout, ANSI_COLOR_GREEN);
    vfprintf(stdout, format, v);
    fprintf(stdout, ANSI_COLOR_RESET);
    fflush(stdout);
    va_end(v);
#else
    (void) format;
#endif
}


