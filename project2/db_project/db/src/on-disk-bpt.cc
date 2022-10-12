#include "on-disk-bpt.h"
#include <iostream>

/* TODO */
// 1. Fix util functions to reference.

slotnum_t cut_leaf(std::vector<slot_t>& slots) {
    slotnum_t num_slots = slots.size();
    slotnum_t cut = 0;
    slotnum_t size = 0;
    for(slotnum_t i = 0; i < num_slots; i++) {
        size += slot_io::get_record_size(&slots[i]) + SLOT_SIZE;
        if(size >= (PAGE_SIZE - PAGE_HEADER_SIZE) / 2) {
            cut = i;
            break;
        }
    }
    return cut;
}

slotnum_t cut_internal() {
    return INTERNAL_ORDER / 2;
}

pagenum_t get_left_idx(int64_t table_id, pagenum_t parent, pagenum_t left) {
    page_t parent_page;
    file_read_page(table_id, parent, &parent_page);
    pagenum_t left_idx = 0;

    uint64_t num_keys = page_io::get_key_count(&parent_page);
    for(pagenum_t i = 0; i < num_keys + 1; ++i) {
        pagenum_t child = page_io::internal::get_child(&parent_page, i);
        if(child == left) {
            left_idx = i;
            break;
        }
    }

    return left_idx;
}

pagenum_t find_leaf(int64_t table_id, pagenum_t root, int64_t key) {
    if(root == 0) return 0;

    pagenum_t c = root;
    while(true) {
        page_t temp_page;
        file_read_page(table_id, c, &temp_page);

        if(page_io::is_leaf(&temp_page)) break;

        pagenum_t num_keys = page_io::get_key_count(&temp_page);
        slotnum_t i = 0;
        while(i < num_keys) {
            int64_t temp_key = page_io::internal::get_key(&temp_page, i);
            if(key >= temp_key) i++;
            else break;
        }

        c = page_io::internal::get_child(&temp_page, i);
    }

    return c;
}

std::pair<pagenum_t, slotnum_t> find(int64_t table_id, pagenum_t root, int64_t key) {
    pagenum_t leaf_n;
    slotnum_t slot_n;

    pagenum_t c = find_leaf(table_id, root, key);
    
    if(c == 0) return std::pair<pagenum_t, slotnum_t>({0, 0});
    
    page_t leaf_page;
    file_read_page(table_id, c, &leaf_page);
    pagenum_t num_keys = page_io::get_key_count(&leaf_page);
    for(slotnum_t i = 0; i < num_keys; ++i) {
        int64_t temp_key = page_io::leaf::get_key(&leaf_page, i);
        if(temp_key == key) {
            leaf_n = c;
            slot_n = i;
            return std::pair<pagenum_t, slotnum_t>({leaf_n, slot_n});
        }
    }

    return std::pair<pagenum_t, slotnum_t>({0, 0});
}

/* Make a leaf node page */
pagenum_t make_leaf(int64_t table_id) {
    pagenum_t leaf_page_num = file_alloc_page(table_id);

    page_t leaf_page;
    page_io::leaf::set_new_leaf_page(&leaf_page);
    file_write_page(table_id, leaf_page_num, &leaf_page);

    return leaf_page_num;
}

pagenum_t make_internal_node(int64_t table_id) {
    pagenum_t internal_page_num = file_alloc_page(table_id);

    page_t internal_page;
    page_io::internal::set_new_internal_page(&internal_page);
    file_write_page(table_id, internal_page_num, &internal_page);

    return internal_page_num;
}

/* Insert key and value into leaf node */
pagenum_t insert_into_leaf(int64_t table_id, pagenum_t leaf, int64_t key, const char* value, uint16_t size) {
    int insertion_point = 0;

    page_t leaf_page;
    file_read_page(table_id, leaf, &leaf_page);
    uint32_t num_keys = page_io::get_key_count(&leaf_page);

    while(insertion_point < num_keys
    && page_io::leaf::get_key(&leaf_page, insertion_point) < key) {
        insertion_point++;
    }

    slotnum_t offset = page_io::leaf::get_offset(&leaf_page, num_keys - 1);

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
    page_io::leaf::update_free_space(&leaf_page, size + SLOT_SIZE);
    page_io::set_key_count(&leaf_page, num_keys + 1);
    file_write_page(table_id, leaf, &leaf_page);

    return leaf;
}

/* Start a new tree */
pagenum_t start_new_tree(int64_t table_id, int64_t key, const char* value, uint16_t size) {
    pagenum_t root = make_leaf(table_id);

    page_t header;
    file_read_page(table_id, 0, &header);
    page_io::header::set_root_page(&header, root);
    file_write_page(table_id, 0, &header);

    slotnum_t offset = (slotnum_t)(PAGE_SIZE - size);
    
    page_t root_page;
    file_read_page(table_id, root, &root_page);

    slot_t slot;
    slot_io::set_new_slot(&slot, key, (slotnum_t)size, offset);
    page_io::leaf::set_slot(&root_page, 0, &slot);
    page_io::leaf::set_record(&root_page, offset, value, size);
    page_io::leaf::update_free_space(&root_page, size + SLOT_SIZE);
    page_io::set_key_count(&root_page, 1);

    file_write_page(table_id, root, &root_page);

    return root;
}

pagenum_t insert_into_leaf_after_splitting(int64_t table_id, pagenum_t root, pagenum_t leaf, int64_t key, const char* value, uint16_t size) {
    int insertion_point = 0;

    page_t leaf_page;
    file_read_page(table_id, leaf, &leaf_page);
    uint32_t num_keys = page_io::get_key_count(&leaf_page);

    while(insertion_point < num_keys
    && page_io::leaf::get_key(&leaf_page, insertion_point) < key) {
        insertion_point++;
    }
    
    std::vector<slot_t> temp_slots(num_keys + 1);
    std::vector<std::string> temp_records(num_keys + 1);
    for(slotnum_t i = 0, j = 0; i < num_keys; ++i, ++j) {
        if(j == insertion_point) j++;
        slot_t& temp_slot = temp_slots[j];
        slot_io::read_slot(&leaf_page, i, &temp_slot);

        slotnum_t offset = slot_io::get_offset(&temp_slot);
        uint16_t record_size = slot_io::get_record_size(&temp_slot);
        char* temp_record = new char[record_size];
        page_io::leaf::get_record(&leaf_page, offset, temp_record, record_size);
        temp_records[j] = std::string(temp_record, record_size);
        delete[] temp_record;
    }
    slot_t& new_slot = temp_slots[insertion_point];
    slot_io::set_new_slot(&new_slot, key, size, (slotnum_t)(PAGE_SIZE - size));
    temp_records[insertion_point] = std::string(value, size);

    slotnum_t split = cut_leaf(temp_slots);

    slotnum_t offset = PAGE_SIZE;
    page_io::leaf::set_free_space(&leaf_page, INITIAL_FREE_SPACE);
    for(slotnum_t i = 0; i < split; ++i) {
        slot_t& temp_slot = temp_slots[i];
        uint16_t record_size = slot_io::get_record_size(&temp_slot);
        offset -= record_size;
        slot_io::set_offset(&temp_slot, offset);
        page_io::leaf::set_slot(&leaf_page, i, &temp_slot);
        page_io::leaf::set_record(&leaf_page, offset, temp_records[i].c_str(), record_size);
        page_io::leaf::update_free_space(&leaf_page, record_size + SLOT_SIZE);
    }
    page_io::set_key_count(&leaf_page, split);

    pagenum_t new_leaf = make_leaf(table_id);
    page_t new_leaf_page;
    file_read_page(table_id, new_leaf, &new_leaf_page);

    offset = PAGE_SIZE;
    for(slotnum_t i = split, j = 0; i < num_keys + 1; ++i, ++j) {
        slot_t& temp_slot = temp_slots[i];
        uint16_t record_size = slot_io::get_record_size(&temp_slot);
        offset -= record_size;
        slot_io::set_offset(&temp_slot, offset);
        page_io::leaf::set_slot(&new_leaf_page, j, &temp_slot);
        page_io::leaf::set_record(&new_leaf_page, offset, temp_records[i].c_str(), record_size);
        page_io::leaf::update_free_space(&new_leaf_page, record_size + SLOT_SIZE);
    }
    page_io::set_key_count(&new_leaf_page, num_keys + 1 - split);

    /* Set right sibling */
    pagenum_t right_sibling = page_io::leaf::get_right_sibling(&leaf_page);
    page_io::leaf::set_right_sibling(&leaf_page, new_leaf);
    page_io::leaf::set_right_sibling(&new_leaf_page, right_sibling);

    /* Set parent */
    pagenum_t parent = page_io::get_parent_page(&leaf_page);
    page_io::set_parent_page(&new_leaf_page, parent);

    /* Write pages */
    file_write_page(table_id, leaf, &leaf_page);
    file_write_page(table_id, new_leaf, &new_leaf_page);

    pagenum_t new_key = page_io::leaf::get_key(&new_leaf_page, 0);

    return insert_into_parent(table_id, root, leaf, new_key, new_leaf);
}

pagenum_t insert_into_node_after_splitting(int64_t table_id, pagenum_t root, pagenum_t old_node, pagenum_t left_index, int64_t key, pagenum_t right) {
    page_t old_node_page;
    file_read_page(table_id, old_node, &old_node_page);
    uint32_t num_keys = page_io::get_key_count(&old_node_page);

    std::vector<pagenum_t> temp_keys(num_keys + 1);
    std::vector<pagenum_t> temp_childs(num_keys + 2);

    for(pagenum_t i = 0, j = 0; i < num_keys + 1; ++i, ++j) {
        if(j == left_index + 1) j++;
        temp_childs[j] = page_io::internal::get_child(&old_node_page, i);
    }

    for(pagenum_t i = 0, j = 0; i < num_keys; ++i, ++j) {
        if(j == left_index) j++;
        temp_keys[j] = page_io::internal::get_key(&old_node_page, i);
    }

    temp_childs[left_index + 1] = right;
    temp_keys[left_index] = key;

    slotnum_t split = cut_internal();
    pagenum_t new_node = make_internal_node(table_id);
    page_t new_node_page;
    file_read_page(table_id, new_node, &new_node_page);

    pagenum_t i, j;
    for(i = 0; i < split - 1; ++i) {
        page_io::internal::set_child(&old_node_page, i, temp_childs[i]);
        page_io::internal::set_key(&old_node_page, i, temp_keys[i]);
    }
    page_io::internal::set_child(&old_node_page, i, temp_childs[i]);
    page_io::set_key_count(&old_node_page, split - 1);

    int64_t prime_key = temp_keys[split - 1];
    for(++i, j = 0; i < INTERNAL_ORDER; ++i, ++j) {
        page_io::internal::set_child(&new_node_page, j, temp_childs[i]);
        page_io::internal::set_key(&new_node_page, j, temp_keys[i]);
    }
    page_io::internal::set_child(&new_node_page, j, temp_childs[i]);
    page_io::set_key_count(&new_node_page, INTERNAL_ORDER - split);

    pagenum_t parent = page_io::get_parent_page(&old_node_page);
    page_io::set_parent_page(&new_node_page, parent);
    num_keys = page_io::get_key_count(&new_node_page);

    for(pagenum_t i = 0; i <= num_keys; ++i) {
        page_t child_page;
        pagenum_t child = page_io::internal::get_child(&new_node_page, i);
        file_read_page(table_id, child, &child_page);
        page_io::set_parent_page(&child_page, new_node);
        file_write_page(table_id, child, &child_page);
    }

    file_write_page(table_id, old_node, &old_node_page);
    file_write_page(table_id, new_node, &new_node_page);

    return insert_into_parent(table_id, root, old_node, prime_key, new_node);
}

pagenum_t insert_into_new_root(int64_t table_id, pagenum_t left, int64_t key, pagenum_t right) {
    pagenum_t root = make_internal_node(table_id);

    page_t header_page;
    file_read_page(table_id, 0, &header_page);
    page_io::header::set_root_page(&header_page, root);
    file_write_page(table_id, 0, &header_page);

    page_t root_page;
    file_read_page(table_id, root, &root_page);

    page_io::internal::set_key(&root_page, 0, key);
    page_io::internal::set_child(&root_page, 0, left);
    page_io::internal::set_child(&root_page, 1, right);
    page_io::set_key_count(&root_page, 1);

    page_t left_page;
    file_read_page(table_id, left, &left_page);
    page_io::set_parent_page(&left_page, root);

    page_t right_page;
    file_read_page(table_id, right, &right_page);
    page_io::set_parent_page(&right_page, root);

    file_write_page(table_id, root, &root_page);
    file_write_page(table_id, left, &left_page);
    file_write_page(table_id, right, &right_page);

    return root;
}

pagenum_t insert_into_parent(int64_t table_id, pagenum_t root, pagenum_t left, int64_t key, pagenum_t right) {
    page_t left_page;
    file_read_page(table_id, left, &left_page);
    pagenum_t parent = page_io::get_parent_page(&left_page);

    /* Case : New root */
    if(parent == 0) {
        return insert_into_new_root(table_id, left, key, right);
    }

    pagenum_t left_index = get_left_idx(table_id, parent, left);
    page_t parent_page;
    file_read_page(table_id, parent, &parent_page);
    uint32_t num_keys = page_io::get_key_count(&parent_page);

    if(num_keys < INTERNAL_ORDER - 1) {
        return insert_into_node(table_id, root, parent, left_index, key, right);
    }

    return insert_into_node_after_splitting(table_id, root, parent, left_index, key, right);
}

pagenum_t insert_into_node(int64_t table_id, pagenum_t root, pagenum_t node, slotnum_t left_idx, int64_t key, pagenum_t right) {
    page_t node_page;
    file_read_page(table_id, node, &node_page);
    uint32_t num_keys = page_io::get_key_count(&node_page);

    for(pagenum_t i = num_keys; i > left_idx; --i) {
        pagenum_t temp_child = page_io::internal::get_child(&node_page, i);
        page_io::internal::set_child(&node_page, i + 1, temp_child);
        pagenum_t temp_key = page_io::internal::get_key(&node_page, i - 1);
        page_io::internal::set_key(&node_page, i, temp_key);
    }

    page_io::internal::set_child(&node_page, left_idx + 1, right);
    page_io::internal::set_key(&node_page, left_idx, key);
    page_io::set_key_count(&node_page, num_keys + 1);

    file_write_page(table_id, node, &node_page);

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
    if(find(table_id, root, key) != std::pair<pagenum_t, slotnum_t>({0, 0})) {
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
    leaf = find_leaf(table_id, root, key);

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

/* * * * * * * * * * * * * * DELETE * * * * * * * * * * * * * */ 
// TODO 

pagenum_t adjust_root(int64_t table_id, pagenum_t root) {
    pagenum_t new_root;

    page_t root_page;
    file_read_page(table_id, root, &root_page);
    uint32_t num_keys = page_io::get_key_count(&root_page);

    if(num_keys > 0) return root;
    
    bool is_leaf = page_io::is_leaf(&root_page);
    if(!is_leaf) {
        new_root = page_io::internal::get_child(&root_page, 0);
        page_t new_root_page;
        file_read_page(table_id, new_root, &new_root_page);
        page_io::set_parent_page(&new_root_page, 0);
        file_write_page(table_id, new_root, &new_root_page);
    }
    else new_root = 0;

    file_free_page(table_id, root);
    
    return new_root;
}

pagenum_t remove_entry_from_node(int64_t table_id, int64_t key, pagenum_t node) {

}

pagenum_t delete_entry(int64_t table_id, pagenum_t root, pagenum_t node, int64_t key) {
    node = remove_entry_from_node(table_id, node, key);

    /* Case : Deletion from the root */
    if(node == root) return adjust_root(table_id, root);

    /* Case : Deletion from the internal node */
    // Do something

    /* Case : Deletion from the leaf node */
    // Do something

    /* Case : Merge */
    // Do something

    /* Case : Redistribute */
    // Do something
}

/* Master deletion function */
pagenum_t master_delete(int64_t table_id, pagenum_t root, int64_t key) {
    pagenum_t key_leaf = find_leaf(table_id, root, key);
    auto location_pair = find(table_id, root, key);

    if(location_pair != std::pair<pagenum_t, slotnum_t>({0, 0})) {
        root = delete_entry(table_id, root, key_leaf, key);
    }

    return root;
}
