#ifndef CS165_HASH_TABLE // This is a header guard. It prevents the header from being included more than once.
#define CS165_HASH_TABLE  

typedef int keyType;
typedef int valType;

// define the nodes in our hashtable's linked lists
typedef struct hashTableNode {
    keyType key1; // the first hashed key
    valType value1; // the first hashed value
    keyType key2; // the second hashed key
    valType value2; // the second hashed value
    struct hashTableNode* next; // pointer to the next node in the linked list
} hashTableNode;

typedef struct hashtable {
// define the components of the hash table here (e.g. the array, bookkeeping for number of elements, etc)
    int num_entries; // number of elements in our hash table
    int size; // number of slots in our hash table
    //int* array; // array for storing our elements
    hashTableNode** array; // array for storing our elements, pointing to hashTableNode
} hashtable;

int allocate(hashtable** ht, int size);
int put(hashtable* ht, keyType key, valType value);
int get(hashtable* ht, keyType key, valType *values, int num_values, int* num_results);
int erase(hashtable* ht, keyType key);
int deallocate(hashtable* ht);

#endif
