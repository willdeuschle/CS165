#ifndef BTREE_H
#define BTREE_H

#include <stdbool.h>
#include "cs165_api.h"

#define PAGESIZE 4096
#define NUM_SIGNPOST_ENTRIES_PER_NODE ((int) (PAGESIZE / sizeof(NodeEntry)))
#define NUM_DATA_ENTRIES_PER_NODE ((int) (PAGESIZE / sizeof(DataEntry)))
#define GREATER_THAN_IDX (-1)
#define NO_IDX (-2)
#define CLUSTERED (-1)
// TODO: increase this for more efficient insertions
#define SIGNPOST_LEEWAY 1

struct Node;

// as a matter of convention, each NodeEntry pointer points to a Node that 
// contains values less than value
typedef struct NodeEntry {
    int value;
    struct Node* node_pointer;
} NodeEntry;

typedef union NodePayload {
    // if it's a leaf node, it has data, otherwise it has signposts
    struct NodeEntry signposts[NUM_SIGNPOST_ENTRIES_PER_NODE];
    DataEntry data[NUM_DATA_ENTRIES_PER_NODE];
} NodePayload;

typedef struct Node {
    int level;
    struct Node* next;
    struct Node* prev;
    struct Node* parent;
    bool is_leaf;
    int parent_index;
    union NodePayload payload;
    int num_entries;
    struct Node* greater_than; // points to all values greater than final NodeEntry
} Node;

typedef struct BTree {
    struct Node* root;
    int height;
    bool is_value_pos;
} BTree;


/* 
 * this function accepts a pointer to a pointer to a btree, initializes a new
 * btree and sets the pointer to point to the pointer to the new btree
 */
void btree_init(BTree** btree_ptr, bool is_value_pos);

/* 
 * this function sets all of a node's pointer's to NULL
 */
void clear_node_pointers(Node* node);

/* 
 * this function creates, initializes, and returns a poiner to a new node
 */
Node* create_new_node(BTree* btree, int level, bool is_leaf, int parent_index, Node* parent, Node* prev, Node* next);

/* 
 * this function finds and returns the node one level deeper that is
 * appropriate for the given value.
 */
Node* next_level(BTree* btree, Node* current_node, int value);

/* 
 * this function inserts a value into a specified btree, and returns the index
 * at which it was inserted (virtual index - as if all the data were contiguous
 * instead of in nodes). also passed a flag "update_on_clustered" that dicates
 * whether or not we need to update the position of other values on insert
 */
int btree_insert(BTree* btree, int value, int pos, bool update_on_clustered);

/* 
 * this function determines whether or not this node points to leaf data
 */
bool is_penultimate_level(BTree* btree, Node* node);

/* 
 * this function gets the next leaf node. manages creating that node if
 * necessary and using the "greater than" node properly. returns false
 * if we cannot get the next node (because we're at the "greater than") or 
 * true on successful creation
 */
Node* get_next_leaf_node(BTree* btree, Node* node);

/* 
 * this function splits data from the prev to the next node, splitting more to
 * the next node if there is not an equitable split. NOTE: this assumes it is
 * safe to split from prev to next and so does not perform any checks.
 * updates the parent signposts as necessary after splitting.
 */
void split_between_prev_and_next(Node* prev, Node* next);

/* 
 * this function splits a leaf node into two nodes, pushing the overflow into the
 * node to the right, and modifies the parent according to these updates
 * to reflect these changes in data location
 * returns a bool indicating whether or not we were able to split
 */
bool split_leaf_node_right(BTree* btree, Node* node_to_split);

/* 
 * this function gets the prev leaf node. manages creating that node if
 * necessary and using the "greater than" node properly. returns NULL
 * if we cannot get the prev node (because we're at the far left leaf node) or 
 * the node itself on successful creation
 */
Node* get_prev_leaf_node(Node* node);

/* 
 * this function splits data from the next to the prev node, splitting more to
 * the prev node if there is not an equitable split. NOTE: this assumes it is
 * safe to split from next to prev and so does not perform any checks.
 * updates the parent signposts as necessary after splitting.
 */
void split_between_next_and_prev(Node* next, Node* prev);

/* 
 * this function splits a leaf node into two nodes, pushing the overflow into the
 * node to the left, and modifies the parent according to these updates
 * to reflect these changes in data location
 * returns a bool indicating whether or not we were able to split
 */
bool split_leaf_node_left(Node* node_to_split);

/* 
 * this function attempts to split right, then attempts to split left if
 * that failed. if both of those fail, it splits the parent. 
 * returns the result of the split
 */
bool split_node(BTree* btree, Node* node_to_split);

/* 
 * this function splits a signpost node into two signpost nodes, pushing the 
 * overflow into the node on the right, and modifies the parent according to 
 * these updates to reflect these changes in signpost location
 * returns a bool indicating whether or not we were able to split
 */
bool split_signpost_node_right(BTree* btree, Node* node_to_split);

/* 
 * this function splits a signpost node into two signpost nodes, pushing the 
 * overflow into the node on the left, and modifies the parent according to 
 * these updates to reflect these changes in signpost location
 * returns a bool indicating whether or not we were able to split
 */
bool split_signpost_node_left(BTree* btree, Node* node_to_split);

/* 
 * this function attempts to split a signpost node right, then left if that
 * fails. if both of those fail, it recursively splits the parent
 * returns the result of the split
 */
bool split_signpost_node(BTree* btree, Node* node_to_split);

/* 
 * this function calculates if a node is full
 */
bool is_full(Node* node);

/*
 * this function calculates if a node should be split
 */
bool should_split(Node* node);

/* 
 * this function shifts signpost information upwards, accounting for the
 * "greater than" node as well.
 */
void shift_signpost_info_up(Node* node, int index, int shift_num);

/* 
 * this function finds the maximum value under a given node, recursively
 * traversing downwards if necessary. this is useful when splitting a signpost
 * node and setting up the new signpost values for the old "greater than" node
 */
int max_value_under_node(Node* node);

/* 
 * this function nullifies old signposts that got moved after a split
 * signpost operation
 */
void nullify_old_signposts(Node* node, int starting_index);

/* 
 * this function increments the level of every node below this node, to
 * be used when splitting the root
 */
void increment_level_below_node(Node* node);

/* 
 * this function updates the signpost of a given node_entry
 */
void update_signpost(NodeEntry* node_entry);

/* 
 * this function finds and returns the rightful prev node for a leaf node,
 * or returns NULL if it shouldn't have one (because it's the far left node)
 */
Node* find_prev_node(Node* node);

/* 
 * this function frees all the allocations associated with a btree
 */
void free_btree(BTree* btree);

/* 
 * this function recursively frees the elements in and below a node
 */
void recursively_free_node(Node* node);

/* 
 * this function searches for val and returns pos
 */
int btree_search_pos(BTree* btree, int val);

/* 
 * this function returns a position vector (assuming we are storing val, id pairs)
 * of the qualifying positions. indicates how many results were found
 */
int* btree_select_range(BTree* btree, int low_value, int high_value, int* num_results);

/* 
 * this function returns the first node with a qualifying DataEntry greater than
 * or equal to low_value. marks the idx of that value
 */
Node* btree_gte_probe(BTree* btree, int low_value, int* first_idx);

/* 
 * this function returns how many entries precede this node
 */
int btree_count_num_prev_entries(Node* node);

/* 
 * this function returns the node that contains the nth entry, and it also
 * marks the location it exists at in that node
 */
Node* btree_find_nth_entry(BTree* btree, int n, int* location);

/* 
 * this function takes a node, likely one that has just been updated in
 * a way that could propagate signpsot changes up the parent, and updates
 * the signposts recursively until reaching the root of the tree (if necessary)
 */
void recursively_update_parent_signposts(Node* node);

/* 
 * this function shifts all the data down in this node entry, decrements the 
 * number of entries, updates the parent signposts, and then decrements
 * the pos of all values larger than the one deleted
 * NOTE: assumes all the pos will be in order (due to clustering on the btree)
 */
void btree_shift_update_decrement(Node* node, int entry_location);

/* 
 * this function updates the pos of all subsequent data starting at 
 * entry_location in node. it continues to subsequent nodes and updates that
 * positional data as well
 * NOTE: assumes all the pos will be in order (due to clustering on the btree)
 */
void btree_decrement_subsequent_pos(Node* node, int entry_location);

/* 
 * this function takes a BTree and a position integer. it
 * iterates through ALL DataEntrys in the leaves, decrementing the pos of all 
 * DataEntrys that have a pos larger than row_pos. it also removes the matching 
 * row_pos and shifts the remaining data downward in that node. finally, it
 * updates the btree as necessary after removing this value (number of entries, signposts)
 */
void btree_delete_and_shift_down_pos(BTree* btree, int row_pos);

/* 
 * this function calculates if a node is more than 2/3 full
 */
//bool above_two_thirds(Node* node);

#endif /* BTREE_H */
