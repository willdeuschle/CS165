#include <stdio.h>
#include <assert.h>
#include "btree.h"

// tests
void test_simple_inserts(void);
void test_simple_overflow(void);
void test_double_overflow(void);
void test_greater_than_overflow(void);
void test_overflow_left(void);
void test_split_root(void);
void test_split_signpost_right_general(void);
void test_split_signpost_left_general(void);
void test_btree_search(void);
void test_btree_range_search(void);

// testing btree functionality
int main(void) {
    /*printf("TEST 1\n");*/
    /*test_simple_inserts();*/
    /*printf("TEST 2\n");*/
    /*test_simple_overflow();*/
    /*printf("TEST 3\n");*/
    /*test_double_overflow();*/
    /*printf("TEST 4\n");*/
    /*test_greater_than_overflow();*/
    /*printf("TEST 5\n");*/
    /*test_overflow_left();*/
    /*printf("TEST 6\n");*/
    /*test_split_root();*/
    /*printf("TEST 7\n");*/
    /*test_split_signpost_right_general();*/
    /*printf("TEST 8\n");*/
    /*test_split_signpost_left_general();*/
    /*printf("TEST 9\n");*/
    /*test_btree_search();*/
    printf("TEST 10\n");
    test_btree_range_search();
    return 0;
}

void test_simple_inserts(void) {
    BTree* btree;
    // false because just the value
    btree_init(&btree, false);

    // btree should be of height 1
    assert(btree->height == 1);
    // root not a leaf
    assert(!btree->root->is_leaf);
    // root has no parent index
    assert(btree->root->parent_index == NO_IDX);

    btree_insert(btree, NUM_DATA_ENTRIES_PER_NODE - 1, -1);
    for (int i = 0; i < NUM_DATA_ENTRIES_PER_NODE - 1; ++i) {
        btree_insert(btree, i, -1);
    }

    // btree should be of height 2
    assert(btree->height == 2);
    // there should only be a single signpost entry in the root
    assert(btree->root->num_entries == 1);
    // the max of that single signpost entry should be NUM_DATA_ENTRIES_PER_NODE
    assert(btree->root->payload.signposts[0].value == NUM_DATA_ENTRIES_PER_NODE);
    // there should be NUM_DATA_ENTRIES_PER_NODE items in the only node in
    // the second level
    assert(btree->root->payload.signposts[0].node_pointer->num_entries == NUM_DATA_ENTRIES_PER_NODE);
    // is a leaf
    assert(btree->root->payload.signposts[0].node_pointer->is_leaf);
    // it's in the 0th index of its parent node
    assert(btree->root->payload.signposts[0].node_pointer->parent_index == 0);

    free_btree(btree);

    return;
}

void test_simple_overflow(void) {
    BTree* btree;
    // false because not (value, pos) pairs
    btree_init(&btree, false);
    btree_insert(btree, NUM_DATA_ENTRIES_PER_NODE, -1);

    // right now should only have a single signpost at NUM_DATA_ENTRIES_PER_NODE + 1
    assert(btree->root->payload.signposts[0].value == NUM_DATA_ENTRIES_PER_NODE + 1);
    // with a single entry
    assert(btree->root->num_entries == 1);
    // that has the right parent index
    assert(btree->root->payload.signposts[0].node_pointer->parent_index == 0);

    // this should cause overflow
    for (int i = 0; i < NUM_DATA_ENTRIES_PER_NODE; ++i) {
        btree_insert(btree, i, -1);
    }

    // should now have two signposts
    assert(btree->root->num_entries == 2);

    // first entry should have a signpost value of (NUM_DATA_ENTRIES_PER_NODE / 2)
    assert(btree->root->payload.signposts[0].value == NUM_DATA_ENTRIES_PER_NODE / 2);
    // still in the 0th parent index
    assert(btree->root->payload.signposts[0].node_pointer->parent_index == 0);

    // second should have a signpost value of NUM_DATA_ENTRIES_PER_NODE + 1
    assert(btree->root->payload.signposts[1].value == NUM_DATA_ENTRIES_PER_NODE + 1);

    // in the first parent index
    assert(btree->root->payload.signposts[1].node_pointer->parent_index == 1);

    // both should be leaf nodes
    assert(btree->root->payload.signposts[0].node_pointer->is_leaf);
    assert(btree->root->payload.signposts[1].node_pointer->is_leaf);


    // they should be connected to their parents
    assert(btree->root->payload.signposts[0].node_pointer->parent == btree->root);
    assert(btree->root->payload.signposts[1].node_pointer->parent == btree->root);

    // they should be connected to each other
    assert(btree->root->payload.signposts[0].node_pointer->next == btree->root->payload.signposts[1].node_pointer);
    assert(btree->root->payload.signposts[1].node_pointer->prev == btree->root->payload.signposts[0].node_pointer);


    // the greater than node should be undefined
    assert(btree->root->greater_than == NULL);
    assert(btree->root->payload.signposts[0].node_pointer->greater_than == NULL);
    assert(btree->root->payload.signposts[1].node_pointer->greater_than == NULL);

    // they should have the proper number of entries
    assert(btree->root->payload.signposts[0].node_pointer->num_entries == NUM_DATA_ENTRIES_PER_NODE / 2);
    assert(btree->root->payload.signposts[1].node_pointer->num_entries == NUM_DATA_ENTRIES_PER_NODE / 2 + 1);

    free_btree(btree);

    return;
}

void test_double_overflow(void) {
    BTree* btree;
    btree_init(&btree, false);

    // fill the first node
    btree_insert(btree, NUM_DATA_ENTRIES_PER_NODE - 1, -1);
    for (int i = NUM_DATA_ENTRIES_PER_NODE - 2; i >= 0; --i) {
        btree_insert(btree, i, -1);
    }
    // should now have one signposts
    assert(btree->root->num_entries == 1);
    // first signpost should be NUM_DATA_ENTRIES_PER_NODE
    assert(btree->root->payload.signposts[0].value == NUM_DATA_ENTRIES_PER_NODE);
    // should be in the 0th parent index
    assert(btree->root->payload.signposts[0].node_pointer->parent_index == 0);

    // fill the second node
    btree_insert(btree, (2 * NUM_DATA_ENTRIES_PER_NODE) - 1, -1);
    for (int i = (2 * NUM_DATA_ENTRIES_PER_NODE) - 2; i >= NUM_DATA_ENTRIES_PER_NODE; --i) {
        btree_insert(btree, i, -1);
    }
    // should now have two signposts
    assert(btree->root->num_entries == 2);
    // second signpost should be 2 * NUM_DATA_ENTRIES_PER_NODE
    assert(btree->root->payload.signposts[0].value == NUM_DATA_ENTRIES_PER_NODE);
    // should be in the 1st parent index
    assert(btree->root->payload.signposts[1].node_pointer->parent_index == 1);

    // now going to insert a single new value, -1
    // it should force a shift over into a third node
    btree_insert(btree, -1, -1);

    // should now be three nodes
    assert(btree->root->num_entries == 3);

    // they should be connected to their parents
    assert(btree->root->payload.signposts[0].node_pointer->parent == btree->root);
    assert(btree->root->payload.signposts[1].node_pointer->parent == btree->root);
    assert(btree->root->payload.signposts[2].node_pointer->parent == btree->root);

    // they should be connected to themselves
    assert(btree->root->payload.signposts[0].node_pointer->next == btree->root->payload.signposts[1].node_pointer);
    assert(btree->root->payload.signposts[1].node_pointer->next == btree->root->payload.signposts[2].node_pointer);
    assert(btree->root->payload.signposts[1].node_pointer->prev == btree->root->payload.signposts[0].node_pointer);
    assert(btree->root->payload.signposts[2].node_pointer->prev == btree->root->payload.signposts[1].node_pointer);

    // they should have the correct number of data entries
    // the third one should have NUM_DATA_ENTRIES_PER_NODE / 2
    assert(btree->root->payload.signposts[2].node_pointer->num_entries == NUM_DATA_ENTRIES_PER_NODE / 2);
    // the second one should have 3/4 * NUM_DATA_ENTRIES_PER_NODE
    assert(btree->root->payload.signposts[1].node_pointer->num_entries == 0.75 * NUM_DATA_ENTRIES_PER_NODE);
    // the first one should have (3/4 * NUM_DATA_ENTRIES_PER_NODE) + 1
    assert(btree->root->payload.signposts[0].node_pointer->num_entries == (0.75 * NUM_DATA_ENTRIES_PER_NODE) + 1);

    // third entry should have a signpost value of 2 * NUM_DATA_ENTRIES_PER_NODE
    assert(btree->root->payload.signposts[2].value == 2 * NUM_DATA_ENTRIES_PER_NODE);

    // second should have a signpost value of 3/2 * NUM_DATA_ENTRIES_PER_NODE
    assert(btree->root->payload.signposts[1].value == 1.5 * NUM_DATA_ENTRIES_PER_NODE);

    // first entry should have a signpost value of 3/4 * NUM_DATA_ENTRIES_PER_NODE
    assert(btree->root->payload.signposts[0].value == 0.75 * NUM_DATA_ENTRIES_PER_NODE);

    // all should be leaf nodes
    assert(btree->root->payload.signposts[0].node_pointer->is_leaf);
    assert(btree->root->payload.signposts[1].node_pointer->is_leaf);
    assert(btree->root->payload.signposts[2].node_pointer->is_leaf);

    // should all have the proper parent index
    assert(btree->root->payload.signposts[0].node_pointer->parent_index == 0);
    assert(btree->root->payload.signposts[1].node_pointer->parent_index == 1);
    assert(btree->root->payload.signposts[2].node_pointer->parent_index == 2);

    // the greater than node should be undefined
    assert(btree->root->greater_than == NULL);
    assert(btree->root->payload.signposts[0].node_pointer->greater_than == NULL);
    assert(btree->root->payload.signposts[1].node_pointer->greater_than == NULL);
    assert(btree->root->payload.signposts[2].node_pointer->greater_than == NULL);

    free_btree(btree);

    return;
}

void test_greater_than_overflow(void) {
    BTree* btree;
    btree_init(&btree, false);

    // fill up all the signposts in the root node
    for (int i = 0; i < NUM_SIGNPOST_ENTRIES_PER_NODE; ++i) {
        btree_insert(btree, (i * NUM_DATA_ENTRIES_PER_NODE) - 1, -1);
        // each one should have the right parent index
        assert(btree->root->payload.signposts[i].node_pointer->parent_index == i);
    }
    
    // should now have NUM_SIGNPOST_ENTRIES_PER_NODE entries
    assert(btree->root->num_entries == NUM_SIGNPOST_ENTRIES_PER_NODE);
    // the last signpost value should be (NUM_DATA_ENTRIES_PER_NODE * (NUM_SIGNPOST_ENTRIES_PER_NODE - 1)) + 1
    assert(btree->root->payload.signposts[NUM_SIGNPOST_ENTRIES_PER_NODE - 1].value == (NUM_DATA_ENTRIES_PER_NODE * (NUM_SIGNPOST_ENTRIES_PER_NODE - 1)));

    // overflow the last node and make sure it goes into the "greater than" node
    // start at the value of the second to last signpost, and insert
    // NUM_DATA_ENTRIES_PER_NODE more nodes
    for (int i = 0; i < NUM_DATA_ENTRIES_PER_NODE; ++i) {
        btree_insert(btree, btree->root->payload.signposts[NUM_SIGNPOST_ENTRIES_PER_NODE - 2].value + i, -1);
    }
    // "greater than" node should now be defined
    assert(btree->root->greater_than != NULL);
    // "greater than" node should have (NUM_DATA_ENTRIES_PER_NODE / 2) + 1 
    // entries (since it also gets the last element)
    assert(btree->root->greater_than->num_entries == (NUM_DATA_ENTRIES_PER_NODE / 2) + 1);

    // final signpost should have been updated to 
    // (((NUM_SIGNPOST_ENTRIES_PER_NODE - 2) * NUM_DATA_ENTRIES_PER_NODE) + (NUM_DATA_ENTRIES_PER_NODE / 2))
    assert(btree->root->payload.signposts[NUM_SIGNPOST_ENTRIES_PER_NODE - 1].value == (((NUM_SIGNPOST_ENTRIES_PER_NODE - 2) * NUM_DATA_ENTRIES_PER_NODE) + (NUM_DATA_ENTRIES_PER_NODE / 2)));

    // final entry should have NUM_DATA_ENTRIES_PER_NODE / 2 elements
    assert(btree->root->payload.signposts[NUM_SIGNPOST_ENTRIES_PER_NODE - 1].node_pointer->num_entries = NUM_DATA_ENTRIES_PER_NODE / 2);

    // last entry and the "greater than" node should have the correct parent indexes
    assert(btree->root->payload.signposts[NUM_SIGNPOST_ENTRIES_PER_NODE - 1].node_pointer->parent_index == NUM_SIGNPOST_ENTRIES_PER_NODE - 1);
    assert(btree->root->greater_than->parent_index == GREATER_THAN_IDX);

    free_btree(btree);

    return;
}

void test_overflow_left(void) {
    BTree* btree;
    btree_init(&btree, false);

    // fill up all the signposts in the root node
    for (int i = 0; i < NUM_SIGNPOST_ENTRIES_PER_NODE; ++i) {
        btree_insert(btree, (i * NUM_DATA_ENTRIES_PER_NODE), -1);
        // each one should have the right parent index
        assert(btree->root->payload.signposts[i].node_pointer->parent_index == i);
    }

    // overflow the "greater than" (which will overflow left)
    int starting_insert = ((NUM_SIGNPOST_ENTRIES_PER_NODE - 1) * NUM_DATA_ENTRIES_PER_NODE) + 1;
    for (int i = starting_insert; i < starting_insert + NUM_DATA_ENTRIES_PER_NODE + 1; ++i) {
        btree_insert(btree, i, -1);
    }

    // make sure they have the right number of entries
    // after this, "greater than" should have (NUM_DATA_ENTRIES_PER_NODE / 2) + 1 entries
    assert(btree->root->greater_than->num_entries == (NUM_DATA_ENTRIES_PER_NODE / 2) + 1);
    // the final node entry should have (NUM_DATA_ENTRIES_PER_NODE / 2) + 1 entries as well
    assert(btree->root->payload.signposts[NUM_SIGNPOST_ENTRIES_PER_NODE - 1].node_pointer->num_entries == (NUM_DATA_ENTRIES_PER_NODE / 2) + 1);

    // make sure the new signpost is correct
    // should be starting_insert + NUM_DATA_ENTRIES_PER_NODE / 2
    assert(btree->root->payload.signposts[NUM_SIGNPOST_ENTRIES_PER_NODE - 1].value = starting_insert + NUM_DATA_ENTRIES_PER_NODE / 2);

    // last entry and the "greater than" node should have the correct parent indexes
    assert(btree->root->payload.signposts[NUM_SIGNPOST_ENTRIES_PER_NODE - 1].node_pointer->parent_index == NUM_SIGNPOST_ENTRIES_PER_NODE - 1);
    assert(btree->root->greater_than->parent_index == GREATER_THAN_IDX);

    free_btree(btree);

    return;
}

void test_split_root(void) {
    BTree* btree;
    btree_init(&btree, false);

    // fill up all the signposts in the root node
    for (int i = 1; i <= NUM_SIGNPOST_ENTRIES_PER_NODE + 1; ++i) {
        btree_insert(btree, (i * NUM_DATA_ENTRIES_PER_NODE), -1);
        if (i <= NUM_SIGNPOST_ENTRIES_PER_NODE) {
            // each one should have the right signpost
            assert(btree->root->payload.signposts[i - 1].value == (i * NUM_DATA_ENTRIES_PER_NODE) + 1);
            // each one should have the right parent index
            assert(btree->root->payload.signposts[i - 1].node_pointer->parent_index == i - 1);
        } else {
            // should have pointer to "greater than"
            assert(btree->root->greater_than->parent_index == GREATER_THAN_IDX);
            assert(btree->root->greater_than->parent == btree->root);
        }
    }
    // total entries in 2 level tree
    int max_entries = (NUM_SIGNPOST_ENTRIES_PER_NODE + 1) * NUM_DATA_ENTRIES_PER_NODE;
    // fill up the entire btree
    for (int i = 0; i < max_entries; ++i) {
        if (i % NUM_DATA_ENTRIES_PER_NODE == 0) {
            continue;
        }
        btree_insert(btree, i, -1);
    }

    // no signposts should have changed
    for (int i = 0; i < NUM_SIGNPOST_ENTRIES_PER_NODE; ++i) {
        // each one should have the right parent index
        assert(btree->root->payload.signposts[i].node_pointer->parent_index == i);
    }
    
    // everything should be full
    for (int i = 0; i < NUM_SIGNPOST_ENTRIES_PER_NODE; ++i) {
        assert(btree->root->payload.signposts[i].node_pointer->num_entries == NUM_DATA_ENTRIES_PER_NODE);
    }
    assert(btree->root->greater_than != NULL);
    assert(btree->root->greater_than->num_entries == NUM_DATA_ENTRIES_PER_NODE);

    // force a signpost split
    // lets force it in the last signpost entry node
    btree_insert(btree, btree->root->payload.signposts[NUM_SIGNPOST_ENTRIES_PER_NODE - 1].value - 1, -1);

    // should now have height of 3
    assert(btree->height == 3);

    // first two levels are signposts, will check the leaves later on
    assert(!btree->root->is_leaf);
    assert(!btree->root->payload.signposts[0].node_pointer->is_leaf);
    assert(!btree->root->payload.signposts[1].node_pointer->is_leaf);
    // also check the level
    assert(btree->root->level == 0);
    assert(btree->root->payload.signposts[0].node_pointer->level == 1);
    assert(btree->root->payload.signposts[1].node_pointer->level == 1);

    // check number of entries at both signpost levels
    assert(btree->root->num_entries == 2);
    assert(btree->root->payload.signposts[0].node_pointer->num_entries == NUM_SIGNPOST_ENTRIES_PER_NODE / 2);
    // plus two because we move one more over, and then we add the element we are trying to add
    assert(btree->root->payload.signposts[1].node_pointer->num_entries == (NUM_SIGNPOST_ENTRIES_PER_NODE / 2) + 2);
    // check connections between signposts and parent
    assert(btree->root->payload.signposts[0].node_pointer->parent = btree->root);
    assert(btree->root->payload.signposts[1].node_pointer->parent = btree->root);
    assert(btree->root->payload.signposts[0].node_pointer->next = btree->root->payload.signposts[1].node_pointer);
    assert(btree->root->payload.signposts[1].node_pointer->prev = btree->root->payload.signposts[0].node_pointer);
    // check signpost values of the parent
    NodeEntry first_signpost = btree->root->payload.signposts[0];
    NodeEntry second_signpost = btree->root->payload.signposts[1];
    Node* second_level_first_node = first_signpost.node_pointer;
    Node* second_level_second_node = second_signpost.node_pointer;
    assert(first_signpost.value == second_level_first_node->payload.signposts[first_signpost.node_pointer->num_entries - 1].value);
    assert(second_signpost.value == second_level_second_node->payload.signposts[second_signpost.node_pointer->num_entries - 1].value);
    // check signpost values of the second level nodes
    Node* third_level_left_last_node = second_level_first_node->payload.signposts[second_level_first_node->num_entries - 1].node_pointer;
    Node* third_level_right_last_node = second_level_second_node->payload.signposts[second_level_second_node->num_entries - 1].node_pointer;
    // remember that the signpost should be one more than the final value
    assert(second_level_first_node->payload.signposts[second_level_first_node->num_entries - 1].value == third_level_left_last_node->payload.data[third_level_left_last_node->num_entries - 1].value + 1);
    assert(second_level_second_node->payload.signposts[second_level_second_node->num_entries - 1].value == third_level_right_last_node->payload.data[third_level_right_last_node->num_entries - 1].value + 1);
    // check "greater than" node information
    assert(btree->root->greater_than == NULL);
    assert(second_level_first_node->greater_than == NULL);
    assert(second_level_second_node->greater_than == NULL);
    // check connections in leaf nodes to each other and parents
    // also make sure they are all leaf nodes
    // first check that they have the correct parent
    for (int i = 0; i < second_level_first_node->num_entries; i++) {
        Node* current_node = second_level_first_node->payload.signposts[i].node_pointer;
        assert(current_node->is_leaf);
        assert(current_node->level == 2);
        assert(current_node->parent == second_level_first_node);
        assert(current_node->parent_index == i);
    }
    for (int i = 0; i < second_level_second_node->num_entries; i++) {
        Node* current_node = second_level_second_node->payload.signposts[i].node_pointer;
        assert(current_node->is_leaf);
        assert(current_node->level == 2);
        assert(current_node->parent == second_level_second_node);
        assert(current_node->parent_index == i);
    }
    // now check between each other
    for (int i = 0; i < second_level_first_node->num_entries - 1; i++) {
        Node* first_node = second_level_first_node->payload.signposts[i].node_pointer;
        Node* second_node = second_level_first_node->payload.signposts[i + 1].node_pointer;
        assert(first_node->next == second_node);
        assert(second_node->prev = first_node);
    }
    for (int i = 0; i < second_level_second_node->num_entries - 1; i++) {
        Node* first_node = second_level_second_node->payload.signposts[i].node_pointer;
        Node* second_node = second_level_second_node->payload.signposts[i + 1].node_pointer;
        assert(first_node->next == second_node);
        assert(second_node->prev = first_node);
    }
    // check that accross the signpost gap, the next and prev pointer is preserved
    assert(third_level_left_last_node->next == second_level_second_node->payload.signposts[0].node_pointer);
    assert(second_level_second_node->payload.signposts[0].node_pointer->prev == third_level_left_last_node);

    // check that the shifted signposts have the correct parent index
    for (int i = 0; i < second_level_first_node->num_entries; ++i) {
        assert(second_level_first_node->payload.signposts[i].node_pointer->parent_index == i);
    }
    for (int i = 0; i < second_level_second_node->num_entries; ++i) {
        assert(second_level_second_node->payload.signposts[i].node_pointer->parent_index == i);
    }

    free_btree(btree);

    return;
}

void test_split_signpost_right_general(void) {
    BTree* btree;
    btree_init(&btree, false);

    // fill up all the signposts in the root node
    for (int i = 1; i <= NUM_SIGNPOST_ENTRIES_PER_NODE + 1; ++i) {
        btree_insert(btree, (i * NUM_DATA_ENTRIES_PER_NODE), -1);
        if (i <= NUM_SIGNPOST_ENTRIES_PER_NODE) {
            // each one should have the right signpost
            assert(btree->root->payload.signposts[i - 1].value == (i * NUM_DATA_ENTRIES_PER_NODE) + 1);
            // each one should have the right parent index
            assert(btree->root->payload.signposts[i - 1].node_pointer->parent_index == i - 1);
        } else {
            // should have pointer to "greater than"
            assert(btree->root->greater_than->parent_index == GREATER_THAN_IDX);
            assert(btree->root->greater_than->parent == btree->root);
        }
    }
    // total entries in 2 level tree
    int max_entries = (NUM_SIGNPOST_ENTRIES_PER_NODE + 1) * NUM_DATA_ENTRIES_PER_NODE;
    // fill up the entire btree
    for (int i = 0; i < max_entries; ++i) {
        if (i % NUM_DATA_ENTRIES_PER_NODE == 0) {
            continue;
        }
        btree_insert(btree, i, -1);
    }

    // no signposts should have changed
    for (int i = 0; i < NUM_SIGNPOST_ENTRIES_PER_NODE; ++i) {
        // each one should have the right parent index
        assert(btree->root->payload.signposts[i].node_pointer->parent_index == i);
    }

    // everything should be full
    for (int i = 0; i < NUM_SIGNPOST_ENTRIES_PER_NODE; ++i) {
        assert(btree->root->payload.signposts[i].node_pointer->num_entries == NUM_DATA_ENTRIES_PER_NODE);
    }
    assert(btree->root->greater_than != NULL);
    assert(btree->root->greater_than->num_entries == NUM_DATA_ENTRIES_PER_NODE);

    // force a signpost split
    int split_val = btree->root->payload.signposts[NUM_SIGNPOST_ENTRIES_PER_NODE - 1].value - 1;
    // lets force it in the last signpost entry node
    btree_insert(btree, split_val, -1);

    // we should now have two signposts in the parent
    assert(btree->root->num_entries == 2);
    
    // want to split again, start it in a fresh node
    int total_needed_to_split_again = (NUM_SIGNPOST_ENTRIES_PER_NODE + 1) * NUM_DATA_ENTRIES_PER_NODE;
    int starting_val = btree->root->payload.signposts[btree->root->num_entries - 1].value;
    int top_val = starting_val + total_needed_to_split_again;
    printf("total needed: %d\n", total_needed_to_split_again);
    // add our top out value
    btree_insert(btree, top_val, -1);

    // we should now have 3 signposts in the parent (we went to the next signpost)
    assert(btree->root->num_entries == 3);

    // now insert everything needed to overflow it
    // try to optimize this by setting up the signposts before inserting
    // all the data
    for (int i = starting_val; i < top_val; i += NUM_DATA_ENTRIES_PER_NODE) {
        btree_insert(btree, i, -1);
    }
    // now insert everything once we have signposts established
    for (int i = starting_val; i < top_val; ++i) {
        if ((i - starting_val) % NUM_DATA_ENTRIES_PER_NODE == 0) {
            // don't insert these signpost values we already put in place
            continue;
        } else {
            if ((i - starting_val) % 10000 == 0) {
                printf("checkpoint: %d\n", i - starting_val);
            }
            btree_insert(btree, i, -1);
        }
    }

    // we should now have 4 signposts in the parent (we split)
    assert(btree->root->num_entries == 4);

    // check the height
    assert(btree->height == 3);
    // check the level
    // root
    assert(btree->root->level == 0);
    // second level signpost
    for (int i = 0; i < btree->root->num_entries; ++i) {
        assert(btree->root->payload.signposts[i].node_pointer->level == 1);
    }
    // third level data
    for (int i = 0; i < btree->root->num_entries; ++i) {
        Node* current_node = btree->root->payload.signposts[i].node_pointer;
        for (int j = 0; j < current_node->num_entries; ++j) {
            assert(current_node->payload.signposts[j].node_pointer->level == 2);
        }
    }
    // check the number of entries at the second level
    // first second level signpost should have NUM_SIGNPOST_ENTRIES_PER_NODE / 2 entries
    assert(btree->root->payload.signposts[0].node_pointer->num_entries == NUM_SIGNPOST_ENTRIES_PER_NODE / 2);
    // second second level signpost should have (NUM_SIGNPOST_ENTRIES_PER_NODE / 2) + 2 entries
    // plus two because we move one more over (through a split), and then we add the element we are trying to add
    assert(btree->root->payload.signposts[1].node_pointer->num_entries == (NUM_SIGNPOST_ENTRIES_PER_NODE / 2) + 2);
    // third second level signpost should have (NUM_SIGNPOST_ENTRIES_PER_NODE / 2) entries
    assert(btree->root->payload.signposts[2].node_pointer->num_entries == NUM_SIGNPOST_ENTRIES_PER_NODE / 2);
    // fourth second level signpost should have (NUM_SIGNPOST_ENTRIES_PER_NODE / 2) + 1 entries
    // plus two because we move one more over (through a split), and then we add the element we are trying to add
    assert(btree->root->payload.signposts[3].node_pointer->num_entries == (NUM_SIGNPOST_ENTRIES_PER_NODE / 2) + 2);
    // check leaf status
    assert(!btree->root->is_leaf);
    for (int i = 0; i < btree->root->num_entries; ++i) {
        assert(!btree->root->payload.signposts[i].node_pointer->is_leaf);
    }
    // third level data should be leaves
    for (int i = 0; i < btree->root->num_entries; ++i) {
        Node* current_node = btree->root->payload.signposts[i].node_pointer;
        for (int j = 0; j < current_node->num_entries; ++j) {
            assert(current_node->payload.signposts[j].node_pointer->is_leaf);
        }
    }
    // check connections between signposts and parent
    for (int i = 0; i < btree->root->num_entries; ++i) {
        assert(btree->root->payload.signposts[i].node_pointer->parent == btree->root);
    }
    // third level data should also be connected to parent properly
    for (int i = 0; i < btree->root->num_entries; ++i) {
        Node* current_node = btree->root->payload.signposts[i].node_pointer;
        for (int j = 0; j < current_node->num_entries; ++j) {
            assert(current_node->payload.signposts[j].node_pointer->parent == current_node);
        }
    }
    // check the signpost values of the root node and second level nodes
    for (int i = 0; i < btree->root->num_entries; ++i) {
        NodeEntry current_node_entry = btree->root->payload.signposts[i];
        assert(current_node_entry.value == max_value_under_node(current_node_entry.node_pointer) + 1);
        // check the second level signposts
        for (int j = 0; j < current_node_entry.node_pointer->num_entries; j++) {
            NodeEntry next_node_entry = current_node_entry.node_pointer->payload.signposts[j];
            assert(next_node_entry.value == max_value_under_node(next_node_entry.node_pointer) + 1);
        }
    }
    // check "greater than" node information
    // root shouldn't have a greater than, nor should any of the second level NodeEntry's
    assert(btree->root->greater_than == NULL);
    for (int i = 0; i < btree->root->num_entries; ++i) {
        assert(btree->root->payload.signposts[i].node_pointer->greater_than == NULL);
    }
    // check connections between second level signpost nodes
    for (int i = 0; i < btree->root->num_entries; ++i) {
        Node* current_node = btree->root->payload.signposts[i].node_pointer;
        if (i == 0) {
            // at the front
            assert(current_node->prev == NULL);
            Node* next_by_connection = current_node->next;
            Node* next_by_parent = btree->root->payload.signposts[i + 1].node_pointer;
            // they should be the same
            assert(next_by_connection == next_by_parent);
        } else if (i == btree->root->num_entries - 1) {
            // at the end
            assert(current_node->next == NULL);
            Node* prev_by_connection = current_node->prev;
            Node* prev_by_parent = btree->root->payload.signposts[i - 1].node_pointer;
            // they should be the same
            assert(prev_by_connection == prev_by_parent);
        } else {
            // in the middle
            // check prev
            Node* prev_by_connection = current_node->prev;
            Node* prev_by_parent = btree->root->payload.signposts[i - 1].node_pointer;
            // they should be the same
            assert(prev_by_connection == prev_by_parent);
            // check next
            Node* next_by_connection = current_node->next;
            Node* next_by_parent = btree->root->payload.signposts[i + 1].node_pointer;
            // they should be the same
            assert(next_by_connection == next_by_parent);
        }
    }
    // check connections in leaf nodes to each other
    for (int i = 0; i < btree->root->num_entries; ++i) {
        Node* current_node = btree->root->payload.signposts[i].node_pointer;
        for (int j = 0; j < current_node->num_entries; ++j) {
            Node* current_leaf_node = current_node->payload.signposts[j].node_pointer;
            if (j == 0) {
                // at the front
                Node* next_by_connection = current_leaf_node->next;
                Node* next_by_parent = current_node->payload.signposts[j + 1].node_pointer;
                // they should be the same
                assert(next_by_connection == next_by_parent);
            } else if (j == current_node->num_entries - 1) {
                // at the end
                Node* prev_by_connection = current_leaf_node->prev;
                Node* prev_by_parent = current_node->payload.signposts[j - 1].node_pointer;
                // they should be the same
                assert(prev_by_connection == prev_by_parent);
            } else {
                // in the middle
                // check prev
                Node* prev_by_connection = current_leaf_node->prev;
                Node* prev_by_parent = current_node->payload.signposts[j - 1].node_pointer;
                // they should be the same
                assert(prev_by_connection == prev_by_parent);
                // check next
                Node* next_by_connection = current_leaf_node->next;
                Node* next_by_parent = current_node->payload.signposts[j + 1].node_pointer;
                // they should be the same
                assert(next_by_connection == next_by_parent);
            }
        }
    }
    // make sure to check connections across signposts
    // there should be three bridges across signposts because we have 4 second level signposts
    Node* first_signpost = btree->root->payload.signposts[0].node_pointer;
    Node* second_signpost = btree->root->payload.signposts[1].node_pointer;
    Node* third_signpost = btree->root->payload.signposts[2].node_pointer;
    Node* fourth_signpost = btree->root->payload.signposts[3].node_pointer;
    // first next->prev
    Node* first_bridge_left_third_level = first_signpost->payload.signposts[first_signpost->num_entries - 1].node_pointer;
    Node* first_bridge_right_third_level = second_signpost->payload.signposts[0].node_pointer;
    assert(first_bridge_left_third_level->next == first_bridge_right_third_level);
    assert(first_bridge_right_third_level->prev == first_bridge_left_third_level);
    // second next->prev
    Node* second_bridge_left_third_level = second_signpost->payload.signposts[second_signpost->num_entries - 1].node_pointer;
    Node* second_bridge_right_third_level = third_signpost->payload.signposts[0].node_pointer;
    assert(second_bridge_left_third_level->next == second_bridge_right_third_level);
    assert(second_bridge_right_third_level->prev == second_bridge_left_third_level);
    // third next->prev
    Node* third_bridge_left_third_level = third_signpost->payload.signposts[third_signpost->num_entries - 1].node_pointer;
    Node* third_bridge_right_third_level = fourth_signpost->payload.signposts[0].node_pointer;
    assert(third_bridge_left_third_level->next == third_bridge_right_third_level);
    assert(third_bridge_right_third_level->prev == third_bridge_left_third_level);

    // check that everything has the correct parent index
    for (int i = 0; i < btree->root->num_entries; ++i) {
        Node* current_node = btree->root->payload.signposts[i].node_pointer;
        assert(current_node->parent_index == i);
        for (int j = 0; j < current_node->num_entries; ++j) {
            Node* child_node = current_node->payload.signposts[j].node_pointer;
            assert(child_node->parent_index == j);
        }
    }

    free_btree(btree);

    return;
}

void test_split_signpost_left_general(void) {
    BTree* btree;
    btree_init(&btree, false);

    // fill up all the signposts in the root node
    for (int i = 1; i <= NUM_SIGNPOST_ENTRIES_PER_NODE + 1; ++i) {
        btree_insert(btree, (i * NUM_DATA_ENTRIES_PER_NODE), -1);
        if (i <= NUM_SIGNPOST_ENTRIES_PER_NODE) {
            // each one should have the right signpost
            assert(btree->root->payload.signposts[i - 1].value == (i * NUM_DATA_ENTRIES_PER_NODE) + 1);
            // each one should have the right parent index
            assert(btree->root->payload.signposts[i - 1].node_pointer->parent_index == i - 1);
        } else {
            // should have pointer to "greater than"
            assert(btree->root->greater_than->parent_index == GREATER_THAN_IDX);
            assert(btree->root->greater_than->parent == btree->root);
        }
    }
    // total entries in 2 level tree
    int max_entries = (NUM_SIGNPOST_ENTRIES_PER_NODE + 1) * NUM_DATA_ENTRIES_PER_NODE;
    // fill up the entire btree
    for (int i = 0; i < max_entries; ++i) {
        if (i % NUM_DATA_ENTRIES_PER_NODE == 0) {
            continue;
        }
        btree_insert(btree, i, -1);
    }

    // no signposts should have changed
    for (int i = 0; i < NUM_SIGNPOST_ENTRIES_PER_NODE; ++i) {
        // each one should have the right parent index
        assert(btree->root->payload.signposts[i].node_pointer->parent_index == i);
    }

    // everything should be full
    for (int i = 0; i < NUM_SIGNPOST_ENTRIES_PER_NODE; ++i) {
        assert(btree->root->payload.signposts[i].node_pointer->num_entries == NUM_DATA_ENTRIES_PER_NODE);
    }
    assert(btree->root->greater_than != NULL);
    assert(btree->root->greater_than->num_entries == NUM_DATA_ENTRIES_PER_NODE);

    // force a signpost split
    int split_val = btree->root->payload.signposts[NUM_SIGNPOST_ENTRIES_PER_NODE - 1].value - 1;
    // lets force it in the last signpost entry node
    btree_insert(btree, split_val, -1);

    // we should now have two signposts in the parent
    assert(btree->root->num_entries == 2);
    
    // now want to fill up all the signpost slots in the root node
    int starting_val = btree->root->payload.signposts[1].value;
    // need to insert NUM_SIGNPOST_ENTRIES_PER_NODE - 2 of them
    for (int i = starting_val; i < starting_val + NUM_SIGNPOST_ENTRIES_PER_NODE - 2; ++i) {
        btree_insert(btree, i, -1);
    }
    // should now have all signposts full, but the "greater than" node is still NULL
    assert(btree->root->num_entries == NUM_SIGNPOST_ENTRIES_PER_NODE);
    assert(btree->root->greater_than == NULL);

    // now overflow the greater than node and force a split left
    int num_that_fit = (NUM_SIGNPOST_ENTRIES_PER_NODE + 1) * NUM_DATA_ENTRIES_PER_NODE;
    // now need a new starting val
    starting_val = btree->root->payload.signposts[btree->root->num_entries - 1].value;
    // top value sought
    int top_insert_val = starting_val + num_that_fit;
    printf("top value %d\n", top_insert_val);

    // prefill the greater than node with the desired signposts
    for (int i = starting_val + NUM_DATA_ENTRIES_PER_NODE; i <= top_insert_val; i += NUM_DATA_ENTRIES_PER_NODE) {
        btree_insert(btree, i, -1);
    }

    // now add the remaining values
    for (int i = starting_val; i <= top_insert_val; ++i) {
        if (i != starting_val && (i - starting_val) % NUM_DATA_ENTRIES_PER_NODE == 0) {
            // don't want to reinsert these
            continue;
        }
        if (i % 10000 == 0) {
            printf("current_value %d\n", i);
        }
        btree_insert(btree, i, -1);
    }

    // "greater than" node is no longer null
    assert(btree->root->greater_than != NULL);

    // still have full root signposts
    assert(btree->root->num_entries == NUM_SIGNPOST_ENTRIES_PER_NODE);

    // check the height
    assert(btree->height == 3);
    // check the level
    // root
    assert(btree->root->level == 0);
    // second level signpost
    for (int i = 0; i < btree->root->num_entries; ++i) {
        assert(btree->root->payload.signposts[i].node_pointer->level == 1);
    }
    // third level data
    for (int i = 0; i < btree->root->num_entries; ++i) {
        Node* current_node = btree->root->payload.signposts[i].node_pointer;
        for (int j = 0; j < current_node->num_entries; ++j) {
            assert(current_node->payload.signposts[j].node_pointer->level == 2);
        }
    }

    // check the number of entries at the second level
    // last second level signpost should have (NUM_SIGNPOST_ENTRIES_PER_NODE / 2) + 1 entries
    // plus 1 because the location we're shifting too gets as much as it can
    assert(btree->root->payload.signposts[btree->root->num_entries - 1].node_pointer->num_entries == (NUM_SIGNPOST_ENTRIES_PER_NODE / 2) + 1);
    // "greater than" second level signpost should have (NUM_SIGNPOST_ENTRIES_PER_NODE / 2) + 2 entries
    // plus 2 because we shifted (NUM_SIGNPOST_ENTRIES_PER_NODE / 2) over, leaving
    // (NUM_SIGNPOST_ENTRIES_PER_NODE / 2) + 1, but then we also inserted another
    // one after the shift
    assert(btree->root->greater_than->num_entries == (NUM_SIGNPOST_ENTRIES_PER_NODE / 2) + 2);
    // check leaf status
    assert(!btree->root->is_leaf);
    for (int i = 0; i < btree->root->num_entries; ++i) {
        assert(!btree->root->payload.signposts[i].node_pointer->is_leaf);
    }
    // third level data should be leaves
    for (int i = 0; i < btree->root->num_entries; ++i) {
        Node* current_node = btree->root->payload.signposts[i].node_pointer;
        for (int j = 0; j < current_node->num_entries; ++j) {
            assert(current_node->payload.signposts[j].node_pointer->is_leaf);
        }
    }
    // check connections between signposts and parent
    for (int i = 0; i < btree->root->num_entries; ++i) {
        assert(btree->root->payload.signposts[i].node_pointer->parent == btree->root);
    }
    // third level data should also be connected to parent properly
    for (int i = 0; i < btree->root->num_entries; ++i) {
        Node* current_node = btree->root->payload.signposts[i].node_pointer;
        for (int j = 0; j < current_node->num_entries; ++j) {
            assert(current_node->payload.signposts[j].node_pointer->parent == current_node);
        }
    }
    // check the signpost values of the root node and second level nodes
    for (int i = 0; i < btree->root->num_entries; ++i) {
        NodeEntry current_node_entry = btree->root->payload.signposts[i];
        assert(current_node_entry.value == max_value_under_node(current_node_entry.node_pointer) + 1);
        // check the second level signposts
        for (int j = 0; j < current_node_entry.node_pointer->num_entries; j++) {
            NodeEntry next_node_entry = current_node_entry.node_pointer->payload.signposts[j];
            assert(next_node_entry.value == max_value_under_node(next_node_entry.node_pointer) + 1);
        }
    }
    // check "greater than" node information
    // root should have a greater than
    assert(btree->root->greater_than != NULL);
    // none of the second level nodes should
    for (int i = 0; i < btree->root->num_entries; ++i) {
        assert(btree->root->payload.signposts[i].node_pointer->greater_than == NULL);
    }
    // check connections between second level signpost nodes
    for (int i = 0; i < btree->root->num_entries; ++i) {
        Node* current_node = btree->root->payload.signposts[i].node_pointer;
        if (i == 0) {
            // at the front
            assert(current_node->prev == NULL);
            Node* next_by_connection = current_node->next;
            Node* next_by_parent = btree->root->payload.signposts[i + 1].node_pointer;
            // they should be the same
            assert(next_by_connection == next_by_parent);
        } else if (i == btree->root->num_entries - 1) {
            // at the end
            // check prev
            Node* prev_by_connection = current_node->prev;
            Node* prev_by_parent = btree->root->payload.signposts[i - 1].node_pointer;
            // they should be the same
            assert(prev_by_connection == prev_by_parent);
            // check next
            Node* next_by_connection = current_node->next;
            Node* next_by_parent = btree->root->greater_than;
            // they should be the same
            assert(next_by_connection == next_by_parent);
        } else {
            // in the middle
            // check prev
            Node* prev_by_connection = current_node->prev;
            Node* prev_by_parent = btree->root->payload.signposts[i - 1].node_pointer;
            // they should be the same
            assert(prev_by_connection == prev_by_parent);
            // check next
            Node* next_by_connection = current_node->next;
            Node* next_by_parent = btree->root->payload.signposts[i + 1].node_pointer;
            // they should be the same
            assert(next_by_connection == next_by_parent);
        }
    }
    // check connections in leaf nodes to each other
    for (int i = 0; i < btree->root->num_entries; ++i) {
        Node* current_node = btree->root->payload.signposts[i].node_pointer;
        if (current_node->num_entries <= 1) {
            // no connections in this case, continue
            continue;
        }
        for (int j = 0; j < current_node->num_entries; ++j) {
            Node* current_leaf_node = current_node->payload.signposts[j].node_pointer;
            if (j == 0) {
                // at the front
                Node* next_by_connection = current_leaf_node->next;
                Node* next_by_parent = current_node->payload.signposts[j + 1].node_pointer;
                // they should be the same
                assert(next_by_connection == next_by_parent);
            } else if (j == current_node->num_entries - 1) {
                // at the end
                Node* prev_by_connection = current_leaf_node->prev;
                Node* prev_by_parent = current_node->payload.signposts[j - 1].node_pointer;
                // they should be the same
                assert(prev_by_connection == prev_by_parent);
            } else {
                // in the middle
                // check prev
                Node* prev_by_connection = current_leaf_node->prev;
                Node* prev_by_parent = current_node->payload.signposts[j - 1].node_pointer;
                // they should be the same
                assert(prev_by_connection == prev_by_parent);
                // check next
                Node* next_by_connection = current_leaf_node->next;
                Node* next_by_parent = current_node->payload.signposts[j + 1].node_pointer;
                // they should be the same
                assert(next_by_connection == next_by_parent);
            }
        }
    }
    // make sure to check connections across signposts
    // there should be a bridge across every one of the leaf nodes
    for (int i = 0; i < btree->root->num_entries; ++i) {
        // get the parent nodes
        Node* first_parent_node = btree->root->payload.signposts[i].node_pointer;
        Node* second_parent_node = NULL;
        if (i == NUM_SIGNPOST_ENTRIES_PER_NODE - 1) {
            // connects to the "greater than" node
            if (btree->root->greater_than == NULL) {
                continue;
            }
            second_parent_node = btree->root->greater_than;
        } else {
            second_parent_node = btree->root->payload.signposts[i + 1].node_pointer;
        }
        // checks the connection between the parent nodes
        assert(first_parent_node->next == second_parent_node);
        assert(second_parent_node->prev == first_parent_node);
        // shouldn't hit this, but make sure they both have entries
        if (first_parent_node->num_entries == 0 || second_parent_node->num_entries == 0) {
            printf("MISSING ENTRY\n");
            continue;
        }
        // get the leaf nodes
        Node* first_leaf_node = NULL;
        Node* second_leaf_node = second_parent_node->payload.signposts[0].node_pointer;
        if (first_parent_node->num_entries == NUM_DATA_ENTRIES_PER_NODE) {
            if (first_parent_node->greater_than) {
                // connects to the "greater than" node
                first_leaf_node = first_parent_node->greater_than;
            } else {
                first_leaf_node = first_parent_node->payload.signposts[first_parent_node->num_entries - 1].node_pointer;
            }
        } else {
            first_leaf_node = first_parent_node->payload.signposts[first_parent_node->num_entries - 1].node_pointer;
        }
        // checks the connection between the leaf nodes
        assert(first_leaf_node->next == second_leaf_node);
        assert(second_leaf_node->prev == first_leaf_node);
    }

    // check that everything has the correct parent index
    for (int i = 0; i < btree->root->num_entries; ++i) {
        Node* current_node = btree->root->payload.signposts[i].node_pointer;
        assert(current_node->parent_index == i);
        for (int j = 0; j < current_node->num_entries; ++j) {
            Node* child_node = current_node->payload.signposts[j].node_pointer;
            assert(child_node->parent_index == j);
        }
    }

    free_btree(btree);

    return;
}

void test_btree_search(void) {
    BTree* btree;
    btree_init(&btree, false);

    // insert 1000 values
    for (int i = 0; i < 1000; ++i) {
        btree_insert(btree, i, i);
    }

    int pos = btree_search_pos(btree, 0);
    assert(pos == 0);
    pos = btree_search_pos(btree, 250);
    assert(pos == 250);
    pos = btree_search_pos(btree, 500);
    assert(pos == 500);
    pos = btree_search_pos(btree, 750);
    assert(pos == 750);

    free_btree(btree);

    return;
}

void test_btree_range_search(void) {
    BTree* btree;
    btree_init(&btree, false);

    // insert 1000 values
    for (int i = 0; i < 1000; ++i) {
        btree_insert(btree, i, i);
    }

    int num_results;
    int* selected_positions = btree_select_range(btree, 800, 810, &num_results);
    // got the correct number of results
    assert(num_results == 10);
    // got the correct positions
    for (int i = 0; i < num_results; ++i) {
        assert(selected_positions[i] == 800 + i);
    }

    free_btree(btree);

    return;
}
