#include "on-disk-bpt.h"

/* TODO */
// 1. Split free space / key update
// 2. Fix util functions to reference.

/* Make a leaf node page */
pagenum_t make_leaf(uint64_t table_id) {
    pagenum_t leaf_page_num = file_alloc_page(table_id);

    page_t leaf_page;
    page_io::leaf::set_new_leaf_page(&leaf_page);
    file_write_page(table_id, leaf_page_num, &leaf_page);

    return leaf_page_num;
}

/* Insert key and value into leaf node */
pagenum_t insert_into_leaf(int64_t table_id, pagenum_t leaf, int64_t key, const char* value, uint16_t size) {
    int insertion_point = 0;

    page_t leaf_page;
    file_read_page(table_id, leaf, &leaf_page);
    uint16_t num_keys = page_io::get_key_count(&leaf_page);

    while(insertion_point < num_keys
    && page_io::leaf::get_key(&leaf_page, insertion_point) < key) {
        insertion_point++;
    }

    slotnum_t offset = page_io::leaf::get_offset(&leaf_page, num_keys);

    for(pagenum_t i = num_keys; i > insertion_point; --i) {
        pagenum_t temp_key = page_io::leaf::get_key(&leaf_page, i - 1);
        slot_t temp_slot;
        slot_io::read_slot(&leaf_page, i - 1, &temp_slot);
        page_io::leaf::set_slot(&leaf_page, i, &temp_slot);
    }

    slot_t new_slot;
    offset -= size;
    slot_io::set_new_slot(&new_slot, key, size, offset);
    page_io::leaf::set_slot(&leaf_page, insertion_point, &new_slot);
    page_io::leaf::set_record(&leaf_page, offset, value, size);

    page_io::set_key_count(&leaf_page, num_keys + 1);
    file_write_page(table_id, leaf, &leaf_page);

    return leaf;
}

/* Start a new tree */
pagenum_t start_new_tree(int64_t table_id, int64_t key, const char* value, uint16_t size) {
    pagenum_t root = make_leaf(table_id);
    slotnum_t offset = (slotnum_t)(PAGE_SIZE - size);
    
    page_t root_page;
    file_read_page(table_id, root, &root_page);

    slot_t slot;
    slot_io::set_new_slot(&slot, key, (slotnum_t)size, offset);
    page_io::leaf::set_slot(&root_page, 0, &slot);
    page_io::leaf::set_record(&root_page, offset, value, size);
    page_io::leaf::update_free_space(&root_page, size);

    file_write_page(table_id, root, &root_page);

    return root;
}

pagenum_t insert_into_leaf_after_splitting(int64_t table_id, pagenum_t root, pagenum_t leaf, int64_t key, const char* value, uint16_t size) {
    pagenum_t new_leaf = make_leaf(table_id);

    int insertion_point = 0;

    page_t leaf_page;
    file_read_page(table_id, leaf, &leaf_page);
    uint16_t num_keys = page_io::get_key_count(&leaf_page);

    while(insertion_point < num_keys
    && page_io::leaf::get_key(&leaf_page, insertion_point) < key) {
        insertion_point++;
    }

    slotnum_t split = cut_leaf(&leaf_page);
    page_t new_leaf_page;
    file_read_page(table_id, new_leaf, &new_leaf_page);

    slotnum_t size_sum = 0;
    for(slotnum_t i = 0; i < split; ++i) {
        pagenum_t temp_key = page_io::leaf::get_key(&leaf_page, i);
        slot_t temp_slot;
        slot_io::read_slot(&leaf_page, i, &temp_slot);
        page_io::leaf::set_slot(&new_leaf_page, i, &temp_slot);
        size_sum += slot_io::get_record_size(&temp_slot);
    }
    slotnum_t offset = (slotnum_t)(PAGE_SIZE - size_sum);
    slot_t insert_slot;
    slot_io::set_new_slot(&insert_slot, key, size, offset);
    page_io::leaf::set_slot(&leaf_page, split, &insert_slot);
    page_io::leaf::set_record(&leaf_page, offset, value, size);
    
    // TODO :
    // 1. update key count
    // 2. update free space

    for(slotnum_t i = split; i < num_keys; ++i) {
        pagenum_t temp_key = page_io::leaf::get_key(&leaf_page, i);
        slot_t temp_slot;
        slot_io::read_slot(&new_leaf_page, i, &temp_slot);
        page_io::leaf::set_slot(&new_leaf_page, i - split, &temp_slot);
    }

    // TODO :
    // 1. update key count
    // 2. update free space
    // 3. set right sibling

    pagenum_t new_key = page_io::leaf::get_key(&new_leaf_page, 0);

    return insert_into_parent(table_id, root, leaf, new_key, new_leaf);
}

pagenum_t insert_into_node_after_splitting(int64_t table_id, pagenum_t root, pagenum_t old_node, pagenum_t left_index, int64_t key, pagenum_t right) {
    pagenum_t new_node = make_internal_node(table_id);
    slotnum_t split = cut_internal();

    // TODO :
    // 1. update old node
    // 2. set prime key
    // 3. update new node
    // 4. set parent

    int64_t prime_key;

    return insert_into_parent(table_id, root, old_node, prime_key, new_node);
}

pagenum_t insert_into_parent(int64_t table_id, pagenum_t root, pagenum_t left, int64_t key, pagenum_t right) {
    page_t left_page;
    file_read_page(table_id, left, &left_page);
    pagenum_t parent = page_io::get_parent_page(&left_page);

    /* Case : New root */
    if(parent == 0) {
        return insert_into_new_root(table_id, left, key, right);
    }

    pagenum_t left_index = get_left_index(parent, left);
    page_t parent_page;
    file_read_page(table_id, parent, &parent_page);
    pagenum_t num_keys = page_io::get_key_count(&parent_page);

    if(num_keys < INTERNAL_ORDER - 1) {
        return insert_into_node(table_id, root, parent, left_index, key, right);
    }

    return insert_into_node_after_splitting(table_id, root, parent, left_index, key, right);
}

pagenum_t insert_into_node(int64_t table_id, pagenum_t root, pagenum_t node, slotnum_t left_idx, int64_t key, pagenum_t right) {
    page_t node_page;
    file_read_page(table_id, node, &node_page);
    pagenum_t num_keys = page_io::get_key_count(&node_page);

    for(slotnum_t i = num_keys; i > left_idx; --i) {
        // TODO
        // 1. update key in internal page
        // 2. update child
    }

    // TODO
    // 1. push new key and child
    // 2. update key count

    return root;
}

/* Master insertion function */
pagenum_t insert(int64_t table_id, pagenum_t root, int64_t key, const char* value, uint16_t size) {
    /* Get fd from table_id through table manager */
    pagenum_t leaf;

    /* The current implementation ignores
     * duplicates.
     */

    // check if key is already in the tree
    if(find(table_id, key) == true) {
        // insertion failed due to duplicate key.
        return root;
    }

    /* Case : the tree does not exist yet.
     * Start a new tree.
     */
    if(root == 0) {
        return start_new_tree(table_id, key, value, size);
    }

    /* Case : the tree already exists. */
    leaf = find_leaf(table_id, key);

    /* Case : leaf has room for key and value. */
    page_t leaf_page;
    file_read_page(table_id, leaf, &leaf_page);
    pagenum_t free_space = page_io::leaf::get_free_space(&leaf_page);
    if(free_space >= size + SLOT_SIZE) {
        return insert_into_leaf(table_id, leaf, key, value, size);
    }

    /* Case : leaf must be split. */
    return insert_into_leaf_after_splitting(table_id, root, leaf, key, value, size);
}
