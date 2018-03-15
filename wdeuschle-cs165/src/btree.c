#include <assert.h>
#include <string.h>
#include "cs165_api.h"
#include "btree.h"
#include "db_helpers.h"
#include "utils.h"
#include <time.h>

#define STARTING_RESULT_CAPACITY 4096
#define STARTING_DUPLICATE_CAPACITY 100

/* 
 * this function accepts a pointer to a pointer to a btree, initializes a new
 * btree and sets the pointer to point to the pointer to the new btree
 */
void btree_init(BTree** btree_ptr, bool is_value_pos) {
    BTree* btree = malloc(sizeof(BTree));
    memset(btree, 0, sizeof(BTree));
    btree->height = 0;
    btree->root = create_new_node(btree, 0, false, NO_IDX, NULL, NULL, NULL);
    btree->is_value_pos = is_value_pos;
    *btree_ptr = btree;
    return;
}

/* 
 * this function sets all of a node's pointer's to NULL
 */
void clear_node_pointers(Node* node) {
    if (!node->is_leaf) {
        for (int i = 0; i < NUM_SIGNPOST_ENTRIES_PER_NODE; ++i) {
            node->payload.signposts[i].node_pointer = NULL;
        }
    }
    // all other pointers
    node->prev = NULL;
    node->next = NULL;
    node->parent = NULL;
    node->greater_than = NULL;
    return;
}

/* 
 * this function creates, initializes, and returns a poiner to a new node
 */
Node* create_new_node(BTree* btree, int level, bool is_leaf, int parent_index, Node* parent, Node* prev, Node* next) {
    Node* node = malloc(sizeof(Node));
    memset(node, 0, sizeof(Node));
    // set leaf status before clearing pointers
    node->is_leaf = is_leaf;
    // give this node a clean slate, set all it's pointers to NULL
    clear_node_pointers(node);
    // initialize
    node->level = level;
    node->parent = parent;
    node->prev = prev;
    node->next = next;
    node->parent_index = parent_index;
    node->num_entries = 0;
    node->greater_than = NULL;

    // tidy up the connections here
    if (next) {
        next->prev = node;
    }
    if (prev) {
        prev->next = node;
    }

    // increment the height
    if (level == btree->height) {
        ++btree->height;
    }

    return node;
}

/* 
 * this function finds and returns the node one level deeper that is
 * appropriate for the given value.
 */
Node* next_level(BTree* btree, Node* current_node, int value) {
    // this can't be a leaf node
    assert(current_node->is_leaf == false);
    // scan until you find a position that's less than value
    for (int i = 0; i < current_node->num_entries; ++i) {
        if (value < current_node->payload.signposts[i].value) {
            return current_node->payload.signposts[i].node_pointer;
        }
    }
    // haven't found anything less yet
    // TODO: even need this if we're going to be splitting when things get too large???
    if (current_node->num_entries == NUM_SIGNPOST_ENTRIES_PER_NODE) {
        // this value is greater than our largest signpost
        if (current_node->greater_than == NULL) {
            log_info("CREATING A GREATER THAN NODE\n");
            // create new node for greater than
            current_node->greater_than = create_new_node(
                btree,
                current_node->level + 1,
                is_penultimate_level(btree, current_node),
                GREATER_THAN_IDX,
                current_node,
                current_node->payload.signposts[current_node->num_entries - 1].node_pointer,
                current_node->next
            );
        }
        return current_node->greater_than;
    } else {
        log_info("adding a new signpost to an existing node\n");
        // get the prev and next nodes if they exist
        Node* prev = NULL;
        Node* next = NULL;
        if (current_node->num_entries > 0) {
            prev = current_node->payload.signposts[current_node->num_entries - 1].node_pointer;
            next = prev->next;
        }
        // create new node, add an entry to the parent (current) node, 
        // increment number of entries in parent (current) node
        Node* new_node = create_new_node(
            btree,
            current_node->level + 1,
            is_penultimate_level(btree, current_node),
            current_node->num_entries,
            current_node,
            prev,
            next
        );
        assert(new_node->prev == prev);
        assert(new_node->next == next);
        assert(new_node->level > 0);
        NodeEntry new_node_entry = { value + SIGNPOST_LEEWAY, new_node };
        current_node->payload.signposts[current_node->num_entries++] = new_node_entry;
        return new_node;
    }
}

/* 
 * this function inserts a value into a specified btree, and returns the index
 * at which it was inserted (virtual index - as if all the data were contiguous
 * instead of in nodes - if clustered, otherwise index in this node)
 */
int btree_insert(BTree* btree, int value, int pos, bool update_on_clustered) {
    Node* current_node = btree->root;
    // find leaf node
    while (current_node->is_leaf == false) {
        current_node = next_level(btree, current_node, value);
    }
    // current_node is now the bottom level entry
    assert(current_node->is_leaf);

    // handle overflowing data: split this node if it's full
    // TODO: split the right way (left or right, split up the tree)
    if (is_full(current_node)) {
        // split the node
        bool success = split_node(btree, current_node);
        // TODO: handle this gracefully
        if (!success) {
            log_err("INSERT FAILED\n");
            abort();
        }
        bool insert_again = true;
        if (current_node->num_entries == NUM_DATA_ENTRIES_PER_NODE - 1) {
            // we only managed to move one item, let's not get stuck in a
            // loop of shifting a single item back and forth
            if (current_node->parent_index == 0 ||
                value >= current_node->parent->payload.signposts[current_node->prev->parent_index].value) {
                // we're either at the 0th node or we just shifted a single
                // value up to the right. either way, avoid infinite loop here
                insert_again = false;
            }
        }

        if (insert_again) {
            // TODO: there might be a better way to do this - ask stratos if we
            // should just be splitting and then inserting
            /*--num_inserts;*/
            int insert_pos = btree_insert(btree, value, pos, update_on_clustered);
            return insert_pos;
        }
    }

    // if this doesn't have a prev node (because it's the first insertion on 
    // a newly created signpost), we may need to connect it to the previous node
    if (current_node->prev == NULL) {
        current_node->prev = find_prev_node(current_node);
        // also need to update the prev node
        if (current_node->prev) {
            current_node->prev->next = current_node;
        } else {
            assert(current_node->parent_index == 0);
        }
    }

    // find location that this value should go
    int insert_pos = find_data_entry_record_position(current_node->payload.data, current_node->num_entries, value);

    // shift data upward based on this index
    // wherever we are inserting it should be less than or equal to the number
    // of values we're already inserted
    assert(insert_pos <= current_node->num_entries);

    if (insert_pos < current_node->num_entries) {
        // need to shift if we're not inserting at the very end of our current data
        shift_data_up(current_node->payload.data, current_node->num_entries, insert_pos, 1);
    }

    // add this value
    current_node->payload.data[insert_pos].value = value;
    // increment the number of entries
    ++current_node->num_entries;

    // OLD
    // if pos is -1, that means we want to store the insert pos (likely because
    // // we are clustered on this btree)
    // if (pos == CLUSTERED) {
    //     // TODO: remove this for clustered btree by tracking whole way through
    //     // NOTE: technically the positions inserted above this will now be out
    //     // of whack but that's not a big concern since we are clustered on this
    //     // index and can recompute the actual location of a given entry when we
    //     // need it
    //     return_index = btree_count_num_prev_entries(current_node) + insert_pos;
    //     current_node->payload.data[insert_pos].pos = return_index;
    // } else {
    //     current_node->payload.data[insert_pos].pos = pos;
    //     // now we need to shift all of the other pos indexes up 1
    //     // TODO: necessary if we're clustered on something else?
    //     // YES - need to add this back
    //     if (update_on_clustered) {
    //         shift_pos_values_up_one(btree, pos, value);

    // NEW
    // set the pos
    current_node->payload.data[insert_pos].pos = pos;
    // shift all of the other pos indexes up 1 if we aren't bulk loading right now
    if (update_on_clustered) {
        shift_pos_values_up_one(btree, pos, value);
    }

    // validate our nodes, don't need to do this, only when debugging
    /*if (current_node->parent_index == GREATER_THAN_IDX) {*/
        /*assert(current_node == current_node->parent->greater_than);*/
    /*} else {*/
        /*assert(current_node == current_node->parent->payload.signposts[current_node->parent_index].node_pointer);*/
    /*}*/
    /*// remove as well*/
    /*if (current_node->parent && current_node->parent->greater_than) {*/
        /*assert(current_node->parent->greater_than == current_node->parent->payload.signposts[255].node_pointer->next);*/
    /*}*/

    // done, return pos we inserted at, this used to be a return index but
    // we managed to improve the performance of this through other means
    return pos;
}

/* 
 * this function determines whether or not this node points to leaf data
 */
bool is_penultimate_level(BTree* btree, Node* node) {
    // if we have a height of 1, that means we only have the root node
    return node->level + 2 == btree->height || btree->height == 1;
}

/* 
 * this function gets the next leaf node. manages creating that node if
 * necessary and using the "greater than" node properly. returns NULL
 * if we cannot get the next node (because we're at the "greater than") or 
 * the node itself on successful creation
 */
Node* get_next_leaf_node(BTree* btree, Node* node) {
    assert(node->is_leaf);

    log_info("Getting next leaf node right\n");
    Node* parent = node->parent;

    // we can't split the "greater than" node to the right
    if (parent->greater_than == node) {
        log_err("CANT SPLIT GREATER THAN LEAF NODE TO THE RIGHT\n");
        return NULL;
    }

    // get the next node, the one we're moving into
    Node* next = NULL;
    // dont check if this is the last entry block
    if (node->parent_index < NUM_SIGNPOST_ENTRIES_PER_NODE - 1) {
        // note that we can't just access node->next because we want
        // to move into the next open slot if possible (which isn't necessarily
        // the next node)
        next = node->parent->payload.signposts[node->parent_index + 1].node_pointer;
    } else if (node->parent_index == NUM_SIGNPOST_ENTRIES_PER_NODE - 1) {
        next = node->parent->greater_than;
    }

    if (next == NULL) {
        // create the next node if it doesn't exist yet
        // update the node's parent index after creating it
        next = create_new_node(
            btree, node->level, node->is_leaf, NO_IDX,
            parent, node, node->next
        );

        // update the parent for this new node
        if (node->parent_index == NUM_SIGNPOST_ENTRIES_PER_NODE - 1) {
            // in this case we just created the "greater than" node
            assert(parent->greater_than == NULL);
            parent->greater_than = next;
            next->parent_index = GREATER_THAN_IDX;
        } else {
            // in this case we just created a new node somewhere in the middle
            // for which we need a new signpost
            // NOTE: the calling code needs to update this signpost
            next->parent_index = node->parent_index + 1;
            parent->payload.signposts[next->parent_index].node_pointer = next;
            // increment the number of entries in the parent node
            ++parent->num_entries;
        }
    }
    // successful
    return next;
}

/* 
 * this function splits data from the prev to the next node, splitting more to
 * the next node if there is not an equitable split. NOTE: this assumes it is
 * safe to split from prev to next and so does not perform any checks.
 * updates the parent signposts as necessary after splitting.
 */
void split_between_prev_and_next(Node* prev, Node* next) {
    // determine portion of data we're moving to next node based on
    // available space in the next node
    int available_space = NUM_DATA_ENTRIES_PER_NODE - next->num_entries;
    // want to shift the remainder (1) more over if there are an uneven number of spaces
    available_space += available_space % 2;
    // cut it in half so that each of the two nodes has the same amount
    // of free space in them
    int num_to_move = available_space / 2;
    // shift the data in the next node up by the number we are moving over
    shift_data_up(next->payload.data, next->num_entries, 0, num_to_move);
    // now move over the final values from the node we are splitting
    for (int i = 0; i < num_to_move; ++i) {
        next->payload.data[num_to_move - 1 - i] = prev->payload.data[prev->num_entries - 1 - i];
    }
    // now update the size of the node we just moved into
    next->num_entries += num_to_move;
    // now update the size of the node we are splitting
    prev->num_entries = prev->num_entries - num_to_move;
    // now update the signposts of the two parent NodeEntrys that may have changed
    // the prev should always have its parent updated
    update_signpost(&prev->parent->payload.signposts[prev->parent_index]);
    if (next->parent_index != GREATER_THAN_IDX) {
        // update next if it's not the "greater than" node
        update_signpost(&next->parent->payload.signposts[next->parent_index]);
    }
    return;
}

/* 
 * this function splits a leaf node into two nodes, pushing the overflow into the
 * node to the right, and modifies the parent according to these updates
 * to reflect these changes in the process
 * returns a bool indicating whether or not we were able to split
 */
bool split_leaf_node_right(BTree* btree, Node* node_to_split) {
    int before_size = node_to_split->num_entries;
    /*printf("Splitting node right\n");*/
    log_info("Splitting node right\n");
    // get the next leaf node to split into
    Node* next = get_next_leaf_node(btree, node_to_split);

    if (next && !should_split(next)) {
        // either everything over one, or if full, return false
        if (is_full(node_to_split->parent)) {
            // return false, can't shift over
            return false;
        } else {
            int next_idx = node_to_split->parent_index + 1;
            assert(next_idx < NUM_SIGNPOST_ENTRIES_PER_NODE);
            // shift everything above node to split over one, then split into
            // the next node. note that we need to update the shifted parent indexes
            Node* parent = node_to_split->parent;
            shift_signpost_info_up(parent, next_idx, 1);
            Node* prev = node_to_split;
            // next will get connected to current node_to_split's next
            Node* next_next = node_to_split->next;
            next = create_new_node(
                btree, node_to_split->level, node_to_split->is_leaf, next_idx,
                parent, prev, next_next
            );
            assert(next->prev == node_to_split);
            assert(next->next = next_next);
            assert(node_to_split->next = next);
            assert(next_next->prev = next);
            // insert that new node into the parent
            node_to_split->parent->payload.signposts[next_idx].node_pointer = next;
            // increment number of entries in the parent
            ++parent->num_entries;
        }
    } else if (next == NULL) {
        // if we can't split, we must be full
        log_err("UNABLE TO SPLIT RIGHT, MUST BE FULL\n");
        return false;
    }
    // now we're fine to split
    assert(next && !is_full(next));
    // simply split into the next node
    split_between_prev_and_next(node_to_split, next);

    assert(node_to_split->next == next);
    assert(next->prev == node_to_split);
    assert(before_size != node_to_split->num_entries);

    return true;
}

/* 
 * this function gets the prev leaf node. manages creating that node if
 * necessary and using the "greater than" node properly. returns NULL
 * if we cannot get the prev node (because we're at the far left leaf node) or 
 * the node itself on successful creation
 */
Node* get_prev_leaf_node(Node* node) {
    assert(node->is_leaf);

    log_info("Getting prev leaf node left\n");
    Node* parent = node->parent;
    // get the prev node, the one we're moving into
    Node* prev = NULL;
    // dont check if this is the 0th entry
    if (node->parent_index > 0) {
        // note that we can't just access node->prev because we want
        // to move into the prev open slot if possible (which isn't necessarily
        // the prev node, but usually will be particularly because we slways
        // split right first)
        prev = parent->payload.signposts[node->parent_index - 1].node_pointer;
    } else if (node->parent_index == GREATER_THAN_IDX) {
        // this is the "greater than" node
        prev = parent->payload.signposts[NUM_SIGNPOST_ENTRIES_PER_NODE - 1].node_pointer;
    }

    // dont need to create here because the nodes should have all been created
    // as we filled things in from the right
    // TODO: are there situations where we do need to create this?
    if (prev == NULL) {
        log_err("ERROR, SPLIT LEFT AND PREV NODE WAS NULL\n");
    }
    return prev;
}

/* 
 * this function splits data from the next to the prev node, splitting more to
 * the prev node if there is not an equitable split. NOTE: this assumes it is
 * safe to split from next to prev and so does not perform any checks.
 * updates the parent signposts as necessary after splitting.
 */
void split_between_next_and_prev(Node* next, Node* prev) {
    // determine portion of data we're moving to prev node based on
    // available space in the prev node
    int available_space = NUM_DATA_ENTRIES_PER_NODE - prev->num_entries;
    // want to shift the remainder (1) more over if there are an uneven number of spaces
    available_space += available_space % 2;
    // cut it in half so that each of the two nodes has the same amount
    // of free space in them
    int num_to_move = available_space / 2;
    // now move over the starting values from the node we are splitting
    for (int i = 0; i < num_to_move; ++i) {
        prev->payload.data[prev->num_entries + i] = next->payload.data[i];
    }
    // now update the size of the node we just moved into
    prev->num_entries += num_to_move;
    // now shift the data in the current node down by the number of nodes we moved over
    shift_data_down(next->payload.data, next->num_entries, num_to_move, num_to_move);
    // now update the size of the node we are splitting
    next->num_entries = next->num_entries - num_to_move;
    // now update the signposts of the single parent NodeEntry that may have
    // changed. only one can have changed when shifting a leaf node left
    update_signpost(&prev->parent->payload.signposts[prev->parent_index]);
    return;
}
/* 
 * this function splits a leaf node into two nodes, pushing the overflow into the
 * node to the left, and modifies the parent according to these updates
 * to reflect these changes in the process
 * returns a bool indicating whether or not we were able to split
 */
bool split_leaf_node_left(Node* node_to_split) {
    int num_before = node_to_split->num_entries;
    /*printf("Splitting node left\n");*/
    log_info("Splitting node left\n");

    // get the prev leaf node to split into
    Node* prev = get_prev_leaf_node(node_to_split);

    // iterative version
    while (prev && is_full(prev)) {
        // if we can't split left immediately, just fail
        return false;
        // continue until we can split
        Node* next = prev;
        prev = get_prev_leaf_node(next);
        if (prev != NULL) {
            assert(next->prev = prev);
            assert(prev->next = next);
        }
    }

    if (prev == NULL) {
        // if we can't split, we must be full
        log_err("UNABLE TO SPLIT LEFT, MUST BE FULL\n");
        return false;
    } else {
        // now split until we split the node we started with
        Node* next = prev->next;
        assert(next->prev == prev);
        assert(prev->next == next);
        while (next != node_to_split) {
            assert(next->prev == prev);
            assert(prev->next == next);
            // split between this pair
            split_between_next_and_prev(next, prev);
            // move forward 1 pair
            next = next->next;
            prev = prev->next;
        }
        assert(node_to_split->prev == prev);
        assert(prev->next == node_to_split);
        // final split, the one we set out to do originally
        split_between_next_and_prev(node_to_split, prev);
    }
    assert(node_to_split->prev == prev);
    assert(prev->next == node_to_split);
    assert(num_before != node_to_split->num_entries);

    return true;
}

/* 
 * this function attempts to split right, then attempts to split left if
 * that failed, then returns the result of the split
 */
bool split_node(BTree* btree, Node* node_to_split) {
    // try spliting right
    bool success = split_leaf_node_right(btree, node_to_split);
    if (!success) {
        // try splitting left if that failed
        success = split_leaf_node_left(node_to_split);
    }
    if (!success) {
        // now we need to split the parent node
        assert(node_to_split->parent);
        success = split_signpost_node(btree, node_to_split->parent);
        // this should succeed
        assert(success);
    }
    return success;
}

/* 
 * this function splits a signpost node into two signpost nodes, pushing the 
 * overflow into the node on the right, and modifies the parent according to 
 * these updates to reflect these changes in signpost location
 * returns a bool indicating whether or not we were able to split
 */
bool split_signpost_node_right(BTree* btree, Node* node_to_split) {
    log_info("Splitting signpost node right\n");
    Node* parent = node_to_split->parent;

    // don't split root as signpost
    if (btree->root == node_to_split) {
        log_err("FAILED, ROOT AS SIGNPOST NODE\n");
        return false;
    }
    // don't split leaf as a signpost
    if (node_to_split->is_leaf) {
        log_err("FAILED, SPLITTING LEAF NODE AS SIGNPOST NODE\n");
        return false;
    }
    // we can't split the "greater than" node to the right
    if (parent->greater_than == node_to_split) {
        log_err("NOT IMPLEMENTED - SPLIT GREATER THAN SIGNPOST NODE TO THE RIGHT\n");
        return false;
    }

    // get the next node, the one we're moving into
    Node* next = NULL;
    // dont check if this is the last entry block
    if (node_to_split->parent_index < NUM_SIGNPOST_ENTRIES_PER_NODE - 1) {
        // note that we can't just access node_to_split->next because we want
        // to move into the next open slot if possible (which isn't necessarily
        // the next node)
        next = node_to_split->parent->payload.signposts[node_to_split->parent_index + 1].node_pointer;
    } else if (node_to_split->parent_index == NUM_SIGNPOST_ENTRIES_PER_NODE - 1) {
        next = node_to_split->parent->greater_than;
    }

    if (next == NULL) {
        // create the next node if it doesn't exist yet
        // update the node's parent index after creating it
        next = create_new_node(
            btree, node_to_split->level, node_to_split->is_leaf, NO_IDX,
            parent, node_to_split, node_to_split->next
        );

        // update the parent for this new node
        if (node_to_split->parent_index == NUM_SIGNPOST_ENTRIES_PER_NODE - 1) {
            // in this case we just created the "greater than" node
            assert(parent->greater_than == NULL);
            parent->greater_than = next;
            next->parent_index = GREATER_THAN_IDX;
            assert(node_to_split->next == next);
            assert(next->prev = node_to_split);
        } else {
            // in this case we just created a new node somewhere in the middle
            // for which we need a new signpost
            // the new signpost value is just going to be the signpost of the 
            // previous node since we're moving the end of that data over
            next->parent_index = node_to_split->parent_index + 1;
            parent->payload.signposts[next->parent_index].node_pointer = next;
            // increment the number of entries in the parent node
            ++parent->num_entries;
            assert(node_to_split->next == next);
            assert(next->prev = node_to_split);
            // REMOVING THIS - no longer need with the new signpost updating scheme
            /*parent->payload.signposts[next->parent_index].value = parent->payload.signposts[node_to_split->parent_index].value;*/
        }
    }

    if (!should_split(next)) {
        // either everything over one, or if full, return false
        if (is_full(node_to_split->parent)) {
            // return false, can't shift over
            return false;
        } else {
            int next_idx = node_to_split->parent_index + 1;
            assert(next_idx < NUM_SIGNPOST_ENTRIES_PER_NODE);
            // shift everything above node to split over one, then split into
            // the next node. note that we need to update the shifted parent indexes
            Node* parent = node_to_split->parent;
            shift_signpost_info_up(parent, next_idx, 1);
            Node* prev = node_to_split;
            // next will get connected to current node_to_split's next
            Node* next_next = node_to_split->next;
            next = create_new_node(
                btree, node_to_split->level, node_to_split->is_leaf, next_idx,
                parent, prev, next_next
            );
            assert(next->prev == node_to_split);
            assert(next->next = next_next);
            assert(node_to_split->next = next);
            assert(next_next->prev = next);
            // insert that new node into the parent
            node_to_split->parent->payload.signposts[next_idx].node_pointer = next;
            // increment number of entries in the parent
            ++parent->num_entries;
        }
    } else if (next == NULL) {
        // if we can't split, we must be full
        log_err("UNABLE TO SPLIT RIGHT, MUST BE FULL\n");
        return false;
    }

    // determine portion of data we're moving to next node based on
    // available space in the next node
    // plus 1 for the "greater than" node
    int available_space = NUM_SIGNPOST_ENTRIES_PER_NODE - next->num_entries + 1;
    // want to shift the remainder (1) more over if there are an uneven number of spaces
    available_space += available_space % 2;
    // cut it in half so that each of the two nodes has the same amount
    // of free space in them
    int num_to_move = available_space / 2;
    // shift the data in the next node up by the number we are moving over
    shift_signpost_info_up(next, 0, num_to_move);
    // now move over the final values from the node we are splitting
    // first have to move the greater than node over
    // it should have a value (eventually may want to change this if we are
    // splitting before we are full)
    assert(node_to_split->greater_than != NULL);
    // need to find the signpost value for this greater than node
    // note that we add one because values in the node should be smaller than
    // the signpost
    // REMOVING THIS - no longer need with the new signpost updating scheme
    /*int signpost_value = max_value_under_node(node_to_split->greater_than) + 1;*/
    assert(node_to_split->greater_than != NULL);
    NodeEntry first_node_moved = { NO_IDX, node_to_split->greater_than };
    // make sure we reparent this node
    first_node_moved.node_pointer->parent_index = num_to_move - 1;
    first_node_moved.node_pointer->parent = next;
    // now we can assign it
    next->payload.signposts[num_to_move - 1] = first_node_moved;
    // increment the number of entries in next
    assert(next != btree->root);
    ++next->num_entries;
    // decrement the number we have to move now
    --num_to_move;
    // and proceeed to move the rest of the data over
    for (int i = 0; i < num_to_move; ++i) {
        int new_parent_index = num_to_move - 1 - i;
        // shift data for signposts
        next->payload.signposts[new_parent_index] = node_to_split->payload.signposts[node_to_split->num_entries - 1 - i];
        // make sure we reparent these nodes we are moving
        next->payload.signposts[new_parent_index].node_pointer->parent_index = new_parent_index;
        next->payload.signposts[new_parent_index].node_pointer->parent = next;
    }
    // now update the size of the node we just moved into
    next->num_entries += num_to_move;
    // now update the size of the node we are splitting
    node_to_split->num_entries = node_to_split->num_entries - num_to_move;
    // some cleanup: nullify all the pointers in the old signpost that are now in the new signpost
    nullify_old_signposts(node_to_split, node_to_split->num_entries);
    // now update signposts
    // REMOVING THIS - no longer need with the new signpost updating scheme
    // should be the same as the value of the signpost in the last entry of the node we split
    /*parent->payload.signposts[node_to_split->parent_index].value = node_to_split->payload.signposts[node_to_split->num_entries - 1].value;*/
    // now update the signposts of the three NodeEntrys that may have changed:
    // - two parent NodeEntrys may have changed
    // - we may need to update the largest NodeEntry moved into
    // the new signpost node we created (this NodeEntry may have been a "greater than" 
    // node from the node_to_split, meaning it wouldn't have complete signpost info)
    // start with this second thing, the newest NodeEntry moved into the new signpost node
    // note: not num_to_move - 1 because we decremented it after moving this node in earlier
    update_signpost(&next->payload.signposts[num_to_move]);
    // now the first thing: update the parent NodeEntrys
    // the node_to_split always needs updating
    update_signpost(&node_to_split->parent->payload.signposts[node_to_split->parent_index]);
    if (next->parent_index != GREATER_THAN_IDX) {
        // update next if it's not the "greater than" node that we moved into
        update_signpost(&next->parent->payload.signposts[next->parent_index]);
    }
    // bridge the signpost gap here
    assert(node_to_split->payload.signposts[node_to_split->num_entries - 1].node_pointer->next == next->payload.signposts[0].node_pointer);
    log_info("Finished splitting signpost node right\n");
    return true;
}

/* 
 * this function splits a signpost node into two signpost nodes, pushing the 
 * overflow into the node on the left, and modifies the parent according to 
 * these updates to reflect these changes in signpost location
 * returns a bool indicating whether or not we were able to split
 */
bool split_signpost_node_left(BTree* btree, Node* node_to_split) {
    /*printf("Splitting signpost node left\n");*/
    log_info("Splitting signpost node left\n");

    // don't split root as signpost
    if (btree->root == node_to_split) {
        log_err("FAILED, ROOT AS SIGNPOST NODE\n");
        return false;
    }
    // don't split leaf as a signpost
    if (node_to_split->is_leaf) {
        log_err("FAILED, SPLITTING LEAF NODE AS SIGNPOST NODE\n");
        return false;
    }
    // we can't split the 0th node to the left
    // note that we also need to check to make sure that it's not the 
    // GREATER_THAN_IDX since we have marked that as being -1
    if (node_to_split->parent_index <= 0 && node_to_split->parent_index != GREATER_THAN_IDX) {
        log_err("NOT IMPLEMENTED - SPLITTING THE 0th SIGNPOST NODE TO THE LEFT\n");
        return false;
    }

    // get the prev node, the one we're moving into
    Node* prev = NULL;
    if (node_to_split->parent_index > 0) {
        // note that we can't just access node_to_split->prev because we want
        // to move into the prev open slot if possible (which isn't necessarily
        // the prev node, but usually will be particularly because we slways
        // split right first)
        prev = node_to_split->parent->payload.signposts[node_to_split->parent_index - 1].node_pointer;
    } else if (node_to_split->parent_index == GREATER_THAN_IDX) {
        // this is the "greater than" node
        prev = node_to_split->parent->payload.signposts[NUM_SIGNPOST_ENTRIES_PER_NODE - 1].node_pointer;
    }

    if (prev == NULL) {
        log_err("ERROR, SPLIT LEFT AND PREV NODE WAS NULL\n");
        return false;
    }

    bool can_split = true;
    // shift left even more if necessary
    if (is_full(prev)) {
        log_info("Full signpost node, splitting left again\n");
        can_split = split_signpost_node_left(btree, prev);
    }
    // make sure we can split before continuing
    if (!can_split) {
        // if we can't split, we must be full
        log_err("UNABLE TO SPLIT LEFT, MUST BE FULL\n");
        return false;
    }

    // determine portion of data we're moving to prev node based on
    // available space in the prev node
    // plus 1 for the "greater than" node
    int available_space = NUM_SIGNPOST_ENTRIES_PER_NODE - prev->num_entries + 1;
    // want to shift the remainder (1) more over if there are an uneven number of spaces
    available_space += available_space % 2;
    // cut it in half so that each of the two nodes has the same amount
    // of free space in them
    int num_to_move = available_space / 2;
    // now move over the starting values from the node we are splitting
    for (int i = 0; i < num_to_move; ++i) {
        if (prev->num_entries + i < NUM_SIGNPOST_ENTRIES_PER_NODE) {
            int new_parent_index = prev->num_entries + i;
            prev->payload.signposts[new_parent_index] = node_to_split->payload.signposts[i];
            // make sure we reparent these nodes we are moving
            prev->payload.signposts[new_parent_index].node_pointer->parent = prev;
            prev->payload.signposts[new_parent_index].node_pointer->parent_index = new_parent_index;
        } else {
            // this must be the last one
            assert(i == num_to_move - 1);
            // going to use "greater than" node, should be null
            assert(prev->greater_than == NULL);
            prev->greater_than = node_to_split->payload.signposts[i].node_pointer;
            // and reparent this node
            prev->greater_than->parent = prev;
            prev->greater_than->parent_index = GREATER_THAN_IDX;
            // decrement the number of entries moved by one because it was put
            // in the "greater than" node
            --prev->num_entries;
        }
    }
    // now update the size of the node we just moved into
    prev->num_entries += num_to_move;

    // PICKUP HERE: not sure if this indexing is correct
    // now shift the NodeEntry values down in the node_to_split
    for (int i = num_to_move; i <= node_to_split->num_entries; ++i) {
        if (i < node_to_split->num_entries) {
            node_to_split->payload.signposts[i - num_to_move] = node_to_split->payload.signposts[i];
            // and reindex these nodes (same parent)
            node_to_split->payload.signposts[i - num_to_move].node_pointer->parent_index = i - num_to_move;
        } else {
            // this should be the last one
            assert(i == node_to_split->num_entries);
            assert(i == NUM_SIGNPOST_ENTRIES_PER_NODE);
            // we should have a "greater than" node to draw from
            node_to_split->payload.signposts[i - num_to_move].node_pointer = node_to_split->greater_than;
            // update the value of this signpost before returning
            node_to_split->payload.signposts[i - num_to_move].value = NO_IDX;
            // now reindex this node
            node_to_split->payload.signposts[i - num_to_move].node_pointer->parent_index = i - num_to_move;
        }
    }
    // decrement the number of entries - note that we decrement 1 fewer than
    // num_to_move because we picked up the "greater_than" value
    node_to_split->num_entries -= num_to_move - 1;
    // some cleanup: nullify the pointers we moved out of node_to_split
    nullify_old_signposts(node_to_split, node_to_split->num_entries);

    // now update the signposts of the three NodeEntrys that may have changed:
    // - two parent NodeEntrys may have changed
    // - we need to update the largest NodeEntry remaining in node_to_split
    // start with this second task
    update_signpost(&node_to_split->payload.signposts[node_to_split->num_entries - 1]);
    // now the first thing: update the parent NodeEntrys
    // the prev always needs updating
    update_signpost(&prev->parent->payload.signposts[prev->parent_index]);
    if (node_to_split->parent_index != GREATER_THAN_IDX) {
        // update node_to_split if it's not the "greater than" node
        update_signpost(&node_to_split->parent->payload.signposts[node_to_split->parent_index]);
    }
    log_info("Finished splitting signpost node left\n");
    return true;
}

/* 
 * this function attempts to split a signpost node right, then left if that
 * fails. if both of those fail, it recursively splits the parent
 * returns the result of the split
 */
bool split_signpost_node(BTree* btree, Node* node_to_split) {
    if (btree->root == node_to_split) {
        printf("SPLITTING THE ROOT\n");
        log_info("SPLITTING THE ROOT\n");
        // splitting the root
        // lets initialize a replacement root
        Node* new_root = create_new_node(btree, 0, false, NO_IDX, NULL, NULL, NULL);
        // create the first node entry for the new root, which is the old root
        // note that we add one because values in the node should be smaller than
        // the signpost
        int max_val = max_value_under_node(node_to_split) + 1;
        NodeEntry first_node_entry =  { max_val, node_to_split };
        new_root->payload.signposts[0] = first_node_entry;
        // update the old root to have it's parent and parent index
        node_to_split->parent = new_root;
        node_to_split->parent_index = 0;
        // update the number of entries in the new root
        ++new_root->num_entries;
        // make the btree root this new node
        btree->root = new_root;
        // height of the tree just increased
        ++btree->height;
        // now we should be able to split this node
        bool success = split_signpost_node_right(btree, node_to_split);
        // this shouldn't fail as we just split the root
        assert(success);
        // increment the level of everything below the new root
        // TODO: can we remove this? I'm not sure we need the level information
        increment_level_below_node(new_root);

        return success;
    } else {
        // just splitting a middling signpost node
        // try spliting right
        bool success = split_signpost_node_right(btree, node_to_split);
        if (!success) {
            // try splitting left if that failed
            success = split_signpost_node_left(btree, node_to_split);
        }
        if (!success) {
            // now we need to recursively split the parent node
            assert(node_to_split->parent);
            success = split_signpost_node(btree, node_to_split->parent);
        }
        return success;
    }
}

/*
 * this function calculates if a node is full
 */
bool is_full(Node* node) {
    assert(node);
    if (node->is_leaf) {
        /*return node->num_entries >= ((2 * NUM_DATA_ENTRIES_PER_NODE) / 3);*/
        return node->num_entries >= NUM_DATA_ENTRIES_PER_NODE;
    } else {
        /*return node->num_entries >= ((2 * NUM_SIGNPOST_ENTRIES_PER_NODE) / 3);*/
        // plus one for the "greater than" pointer
        if (node->num_entries < NUM_SIGNPOST_ENTRIES_PER_NODE) {
            return false;
        } else {
            // should be equal
            assert(node->num_entries == NUM_SIGNPOST_ENTRIES_PER_NODE);
            return node->greater_than != NULL;
        }
    }
}

/*
 * this function calculates if a leaf node should be split
 */
bool should_split(Node* node) {
    if (node->is_leaf) {
        return node->num_entries <= 0; //(NUM_DATA_ENTRIES_PER_NODE / 16);
    } else {
        return node->num_entries <= 0; //(NUM_SIGNPOST_ENTRIES_PER_NODE / 16);
    }
}

/* 
 * this function shifts signpost information upwards, accounting for the
 * "greater than" node as well.
 */
void shift_signpost_info_up(Node* node, int index, int shift_num) {
    // make sure it will fit
    // plus 1 for the "greater than" node
    assert((node->num_entries + shift_num <= NUM_SIGNPOST_ENTRIES_PER_NODE) || (node->num_entries + shift_num <= NUM_SIGNPOST_ENTRIES_PER_NODE + 1 && node->greater_than == NULL));
    // first move the "greater than" node if necessary
    if (node->num_entries + shift_num > NUM_SIGNPOST_ENTRIES_PER_NODE) {
        // need to move one of the entries into the greater than node, but it
        // should only be a single element over
        assert(node->num_entries + shift_num == NUM_SIGNPOST_ENTRIES_PER_NODE + 1);
        // last element's pointer goes here now
        node->greater_than = node->payload.signposts[node->num_entries - 1].node_pointer;
        // update the node to reflect this, decrement the number of entries
        --node->num_entries;
        // should now all fit
        assert(node->num_entries + shift_num == NUM_SIGNPOST_ENTRIES_PER_NODE);
    }
    // shift each data item
    int new_end_idx = node->num_entries - 1 + shift_num;
    int new_beg_idx = index + shift_num;
    for (int i = new_end_idx; i >= new_beg_idx; --i) {
        node->payload.signposts[i] = node->payload.signposts[i - shift_num];
        // reindex this node
        node->payload.signposts[i].node_pointer->parent_index = i;
    }
    return;
}

/* 
 * this function finds the maximum value under a given node, recursively
 * traversing downwards if necessary. this is useful when splitting a signpost
 * node and setting up the new signpost values for the old "greater than" node
 * note: you will want to add 1 to the value that this returns if you are using
 * the function to generate a signpost value
 */
int max_value_under_node(Node* node) {
    if (node->is_leaf) {
        return node->payload.data[node->num_entries - 1].value;
    } else {
        // return the largest node
        if (node->greater_than != NULL) {
            assert(node->num_entries == NUM_SIGNPOST_ENTRIES_PER_NODE);
            return max_value_under_node(node->greater_than);
        } else {
            return max_value_under_node(node->payload.signposts[node->num_entries - 1].node_pointer);
        }
    }
}

/* 
 * this function nullifies old signposts that got moved after a split
 * signpost operation
 */
void nullify_old_signposts(Node* node, int starting_index) {
    assert(!node->is_leaf);
    for (int i = starting_index; i < NUM_SIGNPOST_ENTRIES_PER_NODE; ++i) {
        node->payload.signposts[i].value = NO_IDX;
        node->payload.signposts[i].node_pointer = NULL;
    }
    node->greater_than = NULL;
    return;
}

/* 
 * this function increments the level of every node below this node, to
 * be used when splitting the root
 */
void increment_level_below_node(Node* node) {
    if (node == NULL || node->is_leaf) {
        return;
    }
    for (int i = 0; i < node->num_entries; ++i) {
        ++node->payload.signposts[i].node_pointer->level;
        increment_level_below_node(node->payload.signposts[i].node_pointer);
    }
}

/* 
 * this function updates the signpost of a given node_entry
 */
void update_signpost(NodeEntry* node_entry) {
    // note: we add 1 because values in the child nodes should be smaller 
    // than the signpost
    int new_signpost_value = max_value_under_node(node_entry->node_pointer) + 1;
    node_entry->value = new_signpost_value;
    return;
}


/* 
 * this function finds and returns the rightful prev node for a leaf node,
 * or returns NULL if it shouldn't have one (because it's the far left node)
 */
Node* find_prev_node(Node* node) {
    // should be a leaf node
    assert(node->is_leaf);
    if (node->parent_index == GREATER_THAN_IDX) {
        // should have a full parent
        assert(node->parent->num_entries == NUM_SIGNPOST_ENTRIES_PER_NODE);
        return node->parent->payload.signposts[node->parent->num_entries - 1].node_pointer;
    } else if (node->parent_index > 0) {
        return node->parent->payload.signposts[node->parent_index - 1].node_pointer;
    } else {
        // the parent index is 0
        assert(node->parent_index == 0);
        // make sure this isn't the far left node of the tree
        Node* current_node = node;
        while (current_node->level > 0) {
            if (current_node->parent_index != 0) {
                // not the far left node
                break;
            } else if (current_node->level == 1) {
                // is the far left node in this case
                return NULL;
            } else {
                // move one level higher
                current_node = current_node->parent;
            }
        }
        Node* parent_prev = current_node->prev;
        if (parent_prev->greater_than != NULL) {
            return parent_prev->greater_than;
        } else {
            return parent_prev->payload.signposts[parent_prev->num_entries - 1].node_pointer;
        }
    }
}

/* 
 * this function frees all the allocations associated with a btree
 */
void free_btree(BTree* btree) {
    // recursively free the nodes
    recursively_free_node(btree->root);
    // free the btree itself
    free(btree);
    return;
}

/* 
 * this function recursively frees the elements in and below a node
 */
void recursively_free_node(Node* node) {
    if (!node->is_leaf) {
        for (int i = 0; i < node->num_entries; ++i) {
            recursively_free_node(node->payload.signposts[i].node_pointer);
        }
        if (node->greater_than) {
            recursively_free_node(node->greater_than);
        }
    }
    free(node);
    return;
}

/* 
 * this function searches for val and returns pos
 */
int btree_search_pos(BTree* btree, int val) {
    Node* current_node = btree->root;
    assert(!current_node->is_leaf);
    while (!current_node->is_leaf) {
        bool found_node = false;
        for (int i = 0; i < current_node->num_entries; ++i) {
            if (current_node->payload.signposts[i].value > val) {
                /*printf("FOUND NODE\n");*/
                current_node = current_node->payload.signposts[i].node_pointer;
                found_node = true;
                break;
            }
        }
        if (!found_node) {
            // didnt find a node yet, see if we can go to the "greater than" node
            if (current_node->num_entries == NUM_SIGNPOST_ENTRIES_PER_NODE && current_node->greater_than) {
                current_node = current_node->greater_than;
            } else {
                /*printf("EVERYTHING TOO SMALL\n");*/
                // everything is too small
                return -1;
            }
        } else {
            break;
        }
    }
    // now we have a leaf node
    assert(current_node->is_leaf);
    // find the entry
    for (int i = 0; i < current_node->num_entries; ++i) {
        if (current_node->payload.data[i].value == val) {
            return current_node->payload.data[i].pos;
        } else if (current_node->payload.data[i].value > val) {
            return -1;
        }
    }
    // doesn't exist
    return -1;
}

/* 
 * this function returns a position vector (assuming we are storing val, id pairs)
 * of the qualifying positions. indicates how many results were found
 */
int* btree_select_range(BTree* btree, int low_value, int high_value, int* num_results) {
    // things to eventually return/update for the caller
    int current_pos_vector_sz = STARTING_RESULT_CAPACITY;
    int* position_vector = malloc(current_pos_vector_sz * sizeof(int));
    int results_count = 0;

    int first_idx;
    Node* first_node_with_result = btree_gte_probe(btree, low_value, &first_idx); 
    if (first_node_with_result == NULL || first_idx == -1) {
        // found nothing
        *num_results = 0;
        return NULL;
    }
    assert(first_node_with_result->is_leaf);

    Node* current_node = first_node_with_result;
    int current_idx = first_idx;
    while (current_idx < current_node->num_entries) {
        // check if we need to resize our position_vector
        if (results_count == current_pos_vector_sz) {
            // resize
            position_vector = resize_data(position_vector, &current_pos_vector_sz);
        }
        if (current_node->payload.data[current_idx].value < high_value) {
            // add this to our position vector
            position_vector[results_count++] = current_node->payload.data[current_idx].pos;
        } else {
            // no more values
            break;
        }
        // make sure we move on to the next node when necessary
        if (current_idx == current_node->num_entries - 1) {
            // move to the next node
            current_node = current_node->next;
            // reset the current index
            current_idx = 0;
            continue;
        }
        // increment current_idx
        ++current_idx;
    }

    // updatee the results count
    *num_results = results_count;
    // return the position vector
    return position_vector;
}

/* 
 * this function returns the first node with a qualifying DataEntry greater than
 * or equal to low_value. marks the idx of that value. returns NULL if nothing
 * matches, and mark the first_idx as -1
 */
Node* btree_gte_probe(BTree* btree, int low_value, int* first_idx) {
    Node* current_node = btree->root;
    assert(!current_node->is_leaf);
    while (!current_node->is_leaf) {
        bool found_node = false;
        for (int i = 0; i < current_node->num_entries; ++i) {
            if (current_node->payload.signposts[i].value > low_value) {
                current_node = current_node->payload.signposts[i].node_pointer;
                found_node = true;
                break;
            }
        }
        if (!found_node) {
            // didnt find a node yet, see if we can go to the "greater than" node
            if (current_node->num_entries == NUM_SIGNPOST_ENTRIES_PER_NODE && current_node->greater_than) {
                current_node = current_node->greater_than;
            } else {
                /*printf("EVERYTHING TOO SMALL\n");*/
                // everything is too small
                *first_idx = -1;
                return NULL;
            }
        } else {
            if (current_node->is_leaf) {
                // found leaf, break
                break;
            }
        }
    }
    // now we have a leaf node
    assert(current_node->is_leaf);
    // find the first entry that is >= to low value
    int entry_idx = -1;
    for (int i = 0; i < current_node->num_entries; ++i) {
        if (current_node->payload.data[i].value >= low_value) {
            entry_idx = i;
            break;
        }
    }
    // set that index and return the node
    *first_idx = entry_idx;
    return current_node;
}

/* 
 * this function returns how many entries precede this node
 */
// TODO: pickup here, need to update
int btree_count_num_prev_entries(Node* node) {
    assert(node->is_leaf);
    int count = 0;
    Node* current_node = node->prev;
    while (current_node) {
        count += current_node->num_entries;
        current_node = current_node->prev;
    }
    return count;
}

/* 
 * this function returns the node that contains the nth entry, and it also
 * marks the location it exists at in that node
 */
Node* btree_find_nth_entry(BTree* btree, int n, int* location) {
    // get far left entry
    Node* current_node = btree->root;
    while (!current_node->is_leaf) {
        current_node = current_node->payload.signposts[0].node_pointer;
    }
    // we now have the far left leaf node
    while (n > current_node->num_entries) {
        n -= current_node->num_entries;
        current_node = current_node->next;
    }
    // now in the node in which the nth element lives
    assert(n < current_node->num_entries);
    // thus, the location is n - 1 in this node (-1 because it's an index)
    *location = n - 1;
    // should be the same as n
    assert(current_node->payload.data[n - 1].pos == n);
    return current_node;
}

/* 
 * this function takes a node, likely one that has just been updated in
 * a way that could propagate signpost changes up the parent, and updates
 * the signposts recursively until reaching the root of the tree (if necessary)
 */
void recursively_update_parent_signposts(Node* node) {
    assert(node && node->parent); // need to have a node and a parent
    // need to continue up the tree if we modify this signpost value, check
    // with old signpost value
    Node* parent = node->parent;
    int old_signpost = parent->payload.signposts[node->parent_index].value;
    update_signpost(&parent->payload.signposts[node->parent_index]);
    while (parent->parent && old_signpost != parent->payload.signposts[node->parent_index].value) {
        // update all the info, and update signpost again
        parent = parent->parent;
        node = node->parent;
        old_signpost = parent->payload.signposts[node->parent_index].value;
        update_signpost(&parent->payload.signposts[node->parent_index]);
    }
    return;
}

/* 
 * this function shifts all the data down in this node entry, decrements the 
 * number of entries, updates the parent signposts, and then decrements
 * the pos of all values larger than the one deleted
 * NOTE: assumes all the pos will be in order (due to clustering on the btree)
 */
void btree_shift_update_decrement(Node* node, int entry_location) {
    // first shift data down in this node
    shift_data_down(node->payload.data, node->num_entries, entry_location, 1);
    // decrement the number of entries in this node
    // TODO: what happens if this hits 0?
    --node->num_entries;
    // then update the pos of all the data starting at entry_location
    btree_decrement_subsequent_pos(node, entry_location);
    // then update the parent signposts of this node, needs to be a recursive
    // update and not a simple signpost update because these changes may
    // propagate
    recursively_update_parent_signposts(node);
    return;
}

/* 
 * this function updates the pos of all subsequent data starting at 
 * entry_location in node. it continues to subsequent nodes and updates that
 * positional data as well
 * NOTE: assumes all the pos will be in order (due to clustering on the btree)
 */
void btree_decrement_subsequent_pos(Node* node, int entry_location) {
    assert(node->is_leaf);
    while (node) {
        while (entry_location < node->num_entries) {
            // decrement the pos
            --node->payload.data[entry_location].pos;
            // increment the current entry location
            ++entry_location;
        }
        // move to the next node, reset the entry location
        node = node->next;
        entry_location = 0;
    }
    return;
}

/* 
 * this function takes a BTree and a position integer. it
 * iterates through ALL DataEntrys in the leaves, decrementing the pos of all 
 * DataEntrys that have a pos larger than row_pos. it also removes the matching 
 * row_pos and shifts the remaining data downward in that node. finally, it
 * updates the btree as necessary after removing this value (number of entries, signposts)
 */
void btree_delete_and_shift_down_pos(BTree* btree, int row_pos) {
    // get far left leaf node
    Node* node = btree->root;
    while (!node->is_leaf) {
        node = node->payload.signposts[0].node_pointer;
    }
    assert(node->is_leaf);
    // starting entry location
    int entry_location = 0;
    // we now have the far left leaf node
    while (node) {
        while (entry_location < node->num_entries) {
            if (node->payload.data[entry_location].pos > row_pos) {
                // decrement any pos larger than row_pos (a shift down)
                --node->payload.data[entry_location].pos;
            } else if (node->payload.data[entry_location].pos == row_pos) {
                // entry we want to delete
                // shift the data in this node down to effectively delete it
                if (entry_location == node->num_entries - 1) {
                    // no need to shift here, decrementing the number
                    // of entries does that effectively. shifting would reach
                    // into data we don't have
                } else {
                    // shift data down onto our current entry, effectively
                    // eliminating it
                    shift_data_down(node->payload.data, node->num_entries, entry_location + 1, 1);
                }
                // decrement the number of entries in this node
                --node->num_entries;
                // update parent signposts
                recursively_update_parent_signposts(node);
                // continue from here, no need to update the entry_location this time
                continue;
            }
            // increment the current entry location
            ++entry_location;
        }
        // move to the next node, reset the entry location
        node = node->next;
        entry_location = 0;
    }
    return;
}

/* 
 * this function calculates if a node is more than 2/3 full (dont think we need this)
 */
/*bool above_two_thirds(Node* node) {*/
    /*if (node->is_leaf) {*/
        /*// data entries*/
        /*return ((float) node->num_entries / (float) NUM_DATA_ENTRIES_PER_NODE) > .667;*/
    /*} else {*/
        /*// signpost entries*/
    /*}*/
/*}*/
