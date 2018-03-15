#include <stdlib.h>
#include <limits.h>
#include <stdbool.h>

#include "hash_table.h"

// prototypes
int generateNextDoubledPrime(int prevPrime);
int hashingFn(keyType key, int size);

#define HT_SIZE_RATIO 0.6 // the ratio that determines when we resize our hash table, can play with this to find a nice balance

// Initialize the components of a hashtable.
// The size parameter is the expected number of elements to be inserted.
// This method returns an error code, 0 for success and -1 otherwise (e.g., if the parameter passed to the method is not null, if malloc fails, etc).
int allocate(hashtable** ht, int size) {
    // The next line tells the compiler that we know we haven't used the variable
    // yet so don't issue a warning. You should remove this line once you use
    // the parameter.
    /*(void) ht;*/
    /*(void) size;*/

    // allocate a hashtable and point ht at it
    *ht = (hashtable*) malloc(sizeof(hashtable));
    // set initial values for our hashtable
    (*ht)->num_entries = 0; // number of elements in hashtable
    (*ht)->size = size; // expected size of hashtable
    (*ht)->array = (hashTableNode**) malloc((*ht)->size * sizeof(hashTableNode*)); // allocate initial space for the array in our hashtable, needs to be for hashTableNode
    // is there a better way to initialize pointers?
    for (int i = 0; i < (*ht)->size; i++) {
        (*ht)->array[i] = NULL;
    }
    return 0;
}

// This method inserts a key-value pair into the hash table.
// It returns an error code, 0 for success and -1 otherwise (e.g., if malloc is called and fails).
int put(hashtable* ht, keyType key, valType value) {
    /*(void) ht;*/
    /*(void) key;*/
    /*(void) value;*/

    // determine if we need to resize the hashtable
    // if it is too large...
    // reallocate a new hashtable with a bigger size
    // copy over all the values from the old hashtable
    // free the old hash tables array and then the hashtable itself

    // start by checking the ratio
    if (HT_SIZE_RATIO < ((float) ht->num_entries / ht->size)) {
        // need to resize, move all the elements over, free old hash table

        int oldSize = ht->size; // track the old size
        ht->size = generateNextDoubledPrime(oldSize); // resize
        ht->num_entries = 0; // dump the number of entries
        hashTableNode** oldArray = ht->array; // get reference to our soon-to-be-deprecated array
        ht->array = (hashTableNode**) malloc(ht->size * sizeof(hashTableNode*)); // generate our new, larger array
        // is there a better way to initialize pointers?
        for (int i = 0; i < ht->size; i++) {
            ht->array[i] = NULL;
        }

        // iterate through the old array
        for (int i = 0; i < oldSize; i++) {
            hashTableNode* ptr = oldArray[i]; // access the linked list pointer
            // while there are still nodes in the list
            while (ptr != NULL) {
                hashTableNode* currentNodePtr = ptr; // going to process this node
                ptr = currentNodePtr->next; // move our tracking pointer
                // put key1, value1 and key2, value2 if either one exists
                if (currentNodePtr->key1 != INT_MIN) { // first key, value pair
                    put(ht, currentNodePtr->key1, currentNodePtr->value1); // add node to our new hash table
                }
                if (currentNodePtr->key2 != INT_MIN) { // second key, value pair
                    put(ht, currentNodePtr->key2, currentNodePtr->value2); // add node to our new hash table
                }
                free(currentNodePtr); // free this node
            }
        }

        // once we have added the old nodes to the new array and freed them, we also free the array of pointers
        free(oldArray);
    }

    // after resizing is taken care of, hash the new value we are trying to put
    int arrayIdx = hashingFn(key, ht->size);

    // get the first element from the appropriate linked list (if it exists)
    // note: this is a pointer to a pointer, so that we can manipulate the 
    // underlying data structure instead of a copy of it
    hashTableNode** ptr = &(ht->array[arrayIdx]);

    int needNewNode = true; // determine whether or not we need a new node

    // move through the linked list until we reach the end or find an opening
    while (*ptr != NULL) {
        if ((*ptr)->key1 == INT_MIN) { // check for opening at key1
            needNewNode = false; // no longer need a new node
            (*ptr)->key1 = key; // assign key
            (*ptr)->value1 = value; // assign value
            break; // no longer need to loop
        } else if ((*ptr)->key2 == INT_MIN) { // check for opening at key2
            needNewNode = false; // no longer need a new node
            (*ptr)->key2 = key; // assign key
            (*ptr)->value2 = value; // assign value
            break; // no longer need to loop
        }
        ptr = &((*ptr)->next); // move our pointer along
    }
    
    if (needNewNode == true) { // need new node
        // create the node to add to our hash table
        hashTableNode* nodeToHash = malloc(sizeof(hashTableNode)); // allocate space
        nodeToHash->key1 = key; // assign first key
        nodeToHash->value1 = value; // assign first value
        nodeToHash->key2 = INT_MIN; // nothing for second key, signal with INT_MIN
        nodeToHash->value2 = INT_MIN; // nothing for second value, signal with INT_MIN
        nodeToHash->next = NULL; // make sure next pointer is null
        // set this pointer (end of the linked list) to our new node
        *ptr = nodeToHash;
    }

    // update the number of entries in the hash table
    ht->num_entries++;

    return 0;
}

// This method retrieves entries with a matching key and stores the corresponding values in the
// values array. The size of the values array is given by the parameter
// num_values. If there are more matching entries than num_values, they are not
// stored in the values array to avoid a buffer overflow. The function returns
// the number of matching entries using the num_results pointer. If the value of num_results is greater than
// num_values, the caller can invoke this function again (with a larger buffer)
// to get values that it missed during the first call. 
// This method returns an error code, 0 for success and -1 otherwise (e.g., if the hashtable is not allocated).
int get(hashtable* ht, keyType key, valType *values, int num_values, int* num_results) {
    /*(void) ht;*/
    /*(void) key;*/
    /*(void) values;*/
    /*(void) num_values;*/
    /*(void) num_results;*/

    int numValuesInLinkedList = 0; // keep track of how many things have been hashed here
    int arrayIdx = hashingFn(key, ht->size); // find the slot in the array that this key hashes to
    hashTableNode* ptr = ht->array[arrayIdx]; // follow the pointers in the linked list

    // iterate through the linked list while our pointer is not null
    while (ptr != NULL) {
        // only continue if the keys match
        if (key == ptr->key1) { // check first key
            // only add this value to the array if we haven't exceeded the array's size
            if (numValuesInLinkedList < num_values) {
                values[numValuesInLinkedList] = ptr->value1; // add the value of this node to the values array
            }
            numValuesInLinkedList++; // increment the number of matching keys found, done after we add the latest value
        }
        if (key == ptr->key2) { // check second key
            // only add this value to the array if we haven't exceeded the array's size
            if (numValuesInLinkedList < num_values) {
                values[numValuesInLinkedList] = ptr->value2; // add the value of this node to the values array
            }
            numValuesInLinkedList++; // increment the number of matching keys found, done after we add the latest value
        }
        ptr = ptr->next; // move our pointer along
    }

    *num_results = numValuesInLinkedList; // return the number of values for this key in our hash table using the num_results pointer

    return 0;
}

// This method erases all key-value pairs with a given key from the hash table.
// It returns an error code, 0 for success and -1 otherwise (e.g., if the hashtable is not allocated).
int erase(hashtable* ht, keyType key) {
    /*(void) ht;*/
    /*(void) key;*/

    int arrayIdx = hashingFn(key, ht->size); // find the slot in the array that this key hashes to
    hashTableNode** ptr = &(ht->array[arrayIdx]); // pointer to the initial pointer in the hash table

    // iterate through the linked list looking for our key
    while (*ptr != NULL) {
        // check if key1 or key2 of the node referenced by the current pointer matches what we are trying to erase
        hashTableNode* tempPtr = *ptr; // get a temporary pointer to the node we are erasing
        int destroyNode = false; // flag to determine if this node needs to be eliminated

        // check each key
        if ((*ptr)->key1 == key) { // checking key1
            // set those values to NULL
            (*ptr)->key1 = INT_MIN;
            (*ptr)->value1 = INT_MIN;
            ht->num_entries--; // decrement number of entries in the hash table
            // determine if we can get rid of this node
            if ((*ptr)->key2 == INT_MIN) { // other key, value pair also empty
                destroyNode = true; // can free this node
            }
        }
        if ((*ptr)->key2 == key) { // checking key2
            // set those values to NULL
            (*ptr)->key2 = INT_MIN;
            (*ptr)->value2 = INT_MIN;
            ht->num_entries--; // decrement number of entries in the hash table
            // determine if we can get rid of this node
            if ((*ptr)->key1 == INT_MIN) { // other key, value pair also empty
                destroyNode = true; // can free this node
            }
        }

        // determine if eliminate the current node or just continue
        if (destroyNode) {
            // the node is empty, erase it
            *ptr = tempPtr->next; // reroute the previous node's pointer past the node we are erasing, this takes care of moving our tracking pointer along
            free(tempPtr); // free the matching node
        } else {
            ptr = &((*ptr)->next); // otherwise, move our tracking pointer along
        }
    }

    return 0;
}

// generate the next prime at least double the previous one
int generateNextDoubledPrime(int prevPrime) {
    int nextDoubledPrime = (prevPrime * 2) + 1; // start one higher than previous one doubled
    // simple sieve of Eratosthenes
    while (true) {
        int isPrime = true; // starts true
        for (int i = 2; i * i <= nextDoubledPrime; i++) { // only need to go up to the square root of the number being tested
            if (nextDoubledPrime % i == 0) { // not prime
                nextDoubledPrime++; // move to next number
                isPrime = false; // set flag
                break; // out of for loop
            }
        }
        // we're done if this is still true
        if (isPrime == true) {
            break;
        }
    }
    return nextDoubledPrime;
}

// determine proper bucket in our hash table
int hashingFn(keyType key, int size) {
    return abs(key % size); // simple modulo hash
}

// This method frees all memory occupied by the hash table.
// It returns an error code, 0 for success and -1 otherwise.
int deallocate(hashtable* ht) {
    // This line tells the compiler that we know we haven't used the variable
    // yet so don't issue a warning. You should remove this line once you use
    // the parameter.
    /*(void) ht;*/

    /*for every bucket in our hash table*/
    for (int i = 0; i < ht->size; i++) {
        hashTableNode* currentNodePtr = ht->array[i];
        // for every node in this linked list
        while (currentNodePtr != NULL) {
            // point at the memory we are about to free
            hashTableNode* nodeToFree = currentNodePtr;
            // move our current pointer along
            currentNodePtr = currentNodePtr->next;
            // free the memory
            free(nodeToFree);
        }
    }
    // now free the array itself
    free(ht->array);
    // now free the hashtable
    free(ht);

    return 0;
}
