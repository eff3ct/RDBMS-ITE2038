#include "on-disk-bpt.h"

struct comparison_struct {
    slot_t slot;
    std::string record;

    bool operator<(const comparison_struct& other) const {
        int64_t key1 = slot_io::get_key(&slot);
        int64_t key2 = slot_io::get_key(&other.slot);
        return key1 < key2;
    }
};

slotnum_t cut_leaf(std::vector<slot_t>& slots) {
    slotnum_t num_slots = slots.size();
    slotnum_t cut = 0;
    slotnum_t size = 0;
    for(slotnum_t i = 0; i < num_slots; ++i) {
        size += slot_io::get_record_size(&slots[i]) + SLOT_SIZE;
        if(size >= (PAGE_SIZE - PAGE_HEADER_SIZE) / 2) {
            cut = i;
            break;
        }
    }
    return cut;
}

slotnum_t cut_internal() {
    return INTERNAL_ORDER / 2 - 1;
}

pagenum_t get_left_idx(int64_t table_id, pagenum_t parent, pagenum_t left) {
    buffer_t* parent_page = buffer_manager.buffer_read_page(table_id, parent);
    pagenum_t left_idx = 0;

    uint64_t num_keys = page_io::get_key_count((page_t*)parent_page->frame);
    for(pagenum_t i = 0; i < num_keys + 1; ++i) {
        pagenum_t child = page_io::internal::get_child((page_t*)parent_page->frame, i);
        if(child == left) {
            left_idx = i;
            break;
        }
    }
    
    buffer_manager.unpin_buffer(table_id, parent);
    return left_idx;
}

pagenum_t get_neighbor_idx(int64_t table_id, pagenum_t node) {
    buffer_t* node_page = buffer_manager.buffer_read_page(table_id, node);
    pagenum_t parent = page_io::get_parent_page((page_t*)node_page->frame);
    buffer_manager.unpin_buffer(table_id, node);

    buffer_t* parent_page = buffer_manager.buffer_read_page(table_id, parent);

    uint64_t num_keys = page_io::get_key_count((page_t*)parent_page->frame);
    for(int i = 0; i < num_keys + 1; ++i) {
        pagenum_t child = page_io::internal::get_child((page_t*)parent_page->frame, i);
        if(child == node) {
            buffer_manager.unpin_buffer(table_id, parent);
            return i - 1;
        }
    }

    buffer_manager.unpin_buffer(table_id, parent);
    exit(EXIT_FAILURE);
}

pagenum_t find_leaf(int64_t table_id, pagenum_t root, int64_t key) {
    if(root == 0) return 0;

    pagenum_t c = root;

    while(true) {
        buffer_t* temp_page = buffer_manager.buffer_read_page(table_id, c);

        if(page_io::is_leaf((page_t*)temp_page->frame)) {
            buffer_manager.unpin_buffer(table_id, c);
            break;
        }

        pagenum_t num_keys = page_io::get_key_count((page_t*)temp_page->frame);
        slotnum_t i = 0;
        while(i < num_keys) {
            int64_t temp_key = page_io::internal::get_key((page_t*)temp_page->frame, i);
            if(key >= temp_key) i++;
            else break;
        }

        pagenum_t new_c = page_io::internal::get_child((page_t*)temp_page->frame, i);
        buffer_manager.unpin_buffer(table_id, c);
        c = new_c;
    }

    return c;
}

std::pair<pagenum_t, slotnum_t> find(int64_t table_id, pagenum_t root, int64_t key) {
    pagenum_t leaf_n;
    slotnum_t slot_n;

    pagenum_t c = find_leaf(table_id, root, key);
    
    if(c == 0) return std::pair<pagenum_t, slotnum_t>({0, 0});
    
    buffer_t* leaf_page = buffer_manager.buffer_read_page(table_id, c);
    pagenum_t num_keys = page_io::get_key_count((page_t*)leaf_page->frame);
    for(slotnum_t i = 0; i < num_keys; ++i) {
        int64_t temp_key = page_io::leaf::get_key((page_t*)leaf_page->frame, i);
        if(temp_key == key) {
            buffer_manager.unpin_buffer(table_id, c);
            leaf_n = c;
            slot_n = i;
            return std::pair<pagenum_t, slotnum_t>({leaf_n, slot_n});
        }
    }

    buffer_manager.unpin_buffer(table_id, c);

    return std::pair<pagenum_t, slotnum_t>({0, 0});
}

/* Make a leaf node page */
pagenum_t make_leaf(int64_t table_id) {
    pagenum_t leaf_page_num = buffer_manager.buffer_alloc_page(table_id);

    buffer_t* leaf_page = buffer_manager.buffer_read_page(table_id, leaf_page_num);
    buffer_manager.buffer_write_page(table_id, leaf_page_num);
    page_io::leaf::set_new_leaf_page((page_t*)leaf_page->frame);
    buffer_manager.unpin_buffer(table_id, leaf_page_num);

    return leaf_page_num;
}

pagenum_t make_internal_node(int64_t table_id) {
    pagenum_t internal_page_num = buffer_manager.buffer_alloc_page(table_id);

    buffer_t* internal_page = buffer_manager.buffer_read_page(table_id, internal_page_num);
    buffer_manager.buffer_write_page(table_id, internal_page_num);
    page_io::internal::set_new_internal_page((page_t*)internal_page->frame);
    buffer_manager.unpin_buffer(table_id, internal_page_num);

    return internal_page_num;
}

/* Insert key and value into leaf node */
pagenum_t insert_into_leaf(int64_t table_id, pagenum_t leaf, int64_t key, const char* value, uint16_t size) {
    buffer_t* leaf_page = buffer_manager.buffer_read_page(table_id, leaf);

    uint32_t num_keys = page_io::get_key_count((page_t*)leaf_page->frame);

    std::vector<comparison_struct> slots(num_keys + 1);

    for(pagenum_t i = 0; i < num_keys; ++i) {
        slot_io::read_slot((page_t*)leaf_page->frame, i, &slots[i].slot);
        slotnum_t offset = slot_io::get_offset(&slots[i].slot);
        slotnum_t cur_size = slot_io::get_record_size(&slots[i].slot);
        char* temp_record = new char[cur_size];
        page_io::leaf::get_record((page_t*)leaf_page->frame, offset, temp_record, cur_size);
        slots[i].record = std::string(temp_record, cur_size);
        delete[] temp_record;
    }
    slot_io::set_new_slot(&slots.back().slot, key, size, 0);
    slots.back().record = std::string(value, size);

    std::sort(slots.begin(), slots.end());

    buffer_manager.buffer_write_page(table_id, leaf);
    page_io::leaf::set_free_space((page_t*)leaf_page->frame, INITIAL_FREE_SPACE);
    slotnum_t offset = PAGE_SIZE;
    for(pagenum_t i = 0; i < num_keys + 1; ++i) {
        // Set slot
        slotnum_t cur_size = slot_io::get_record_size(&slots[i].slot);
        offset -= cur_size;
        slot_io::set_offset(&slots[i].slot, offset);
        page_io::leaf::set_slot((page_t*)leaf_page->frame, i, &slots[i].slot);

        // Set record
        page_io::leaf::set_record((page_t*)leaf_page->frame, offset, slots[i].record.c_str(), cur_size);

        // Set free space
        page_io::leaf::update_free_space((page_t*)leaf_page->frame, cur_size + SLOT_SIZE);
    }

    page_io::set_key_count((page_t*)leaf_page->frame, num_keys + 1);

    buffer_manager.unpin_buffer(table_id, leaf);

    return leaf;
}

/* Start a new tree */
pagenum_t start_new_tree(int64_t table_id, int64_t key, const char* value, uint16_t size) {
    pagenum_t root = make_leaf(table_id);

    buffer_t* header = buffer_manager.buffer_read_page(table_id, 0);
    buffer_manager.buffer_write_page(table_id, 0);
    page_io::header::set_root_page((page_t*)header->frame, root);
    buffer_manager.unpin_buffer(table_id, 0);

    slotnum_t offset = (slotnum_t)(PAGE_SIZE - size);
    
    buffer_t* root_page = buffer_manager.buffer_read_page(table_id, root);

    buffer_manager.buffer_write_page(table_id, root);
    slot_t slot;
    slot_io::set_new_slot(&slot, key, (slotnum_t)size, offset);
    page_io::leaf::set_slot((page_t*)root_page->frame, 0, &slot);
    page_io::leaf::set_record((page_t*)root_page->frame, offset, value, size);
    page_io::leaf::update_free_space((page_t*)root_page->frame, size + SLOT_SIZE);
    page_io::set_key_count((page_t*)root_page->frame, 1);
    buffer_manager.unpin_buffer(table_id, root);
    
    return root;
}

pagenum_t insert_into_leaf_after_splitting(int64_t table_id, pagenum_t root, pagenum_t leaf, int64_t key, const char* value, uint16_t size) {
    int insertion_point = 0;

    buffer_t* leaf_page = buffer_manager.buffer_read_page(table_id, leaf);
    uint32_t num_keys = page_io::get_key_count((page_t*)leaf_page->frame);

    while(insertion_point < num_keys
    && page_io::leaf::get_key((page_t*)leaf_page->frame, insertion_point) < key) {
        insertion_point++;
    }
    
    std::vector<slot_t> temp_slots(num_keys + 1);
    std::vector<std::string> temp_records(num_keys + 1);
    for(slotnum_t i = 0, j = 0; i < num_keys; ++i, ++j) {
        if(j == insertion_point) j++;
        slot_t& temp_slot = temp_slots[j];
        slot_io::read_slot((page_t*)leaf_page->frame, i, &temp_slot);

        slotnum_t offset = slot_io::get_offset(&temp_slot);
        uint16_t record_size = slot_io::get_record_size(&temp_slot);
        char* temp_record = new char[record_size];
        page_io::leaf::get_record((page_t*)leaf_page->frame, offset, temp_record, record_size);
        temp_records[j] = std::string(temp_record, record_size);
        delete[] temp_record;
    }
    slot_t& new_slot = temp_slots[insertion_point];
    slot_io::set_new_slot(&new_slot, key, size, (slotnum_t)(PAGE_SIZE - size));
    temp_records[insertion_point] = std::string(value, size);

    slotnum_t split = cut_leaf(temp_slots);

    slotnum_t offset = PAGE_SIZE;
    
    buffer_manager.buffer_write_page(table_id, leaf);
    page_io::leaf::set_free_space((page_t*)leaf_page->frame, INITIAL_FREE_SPACE);
    for(slotnum_t i = 0; i < split; ++i) {
        slot_t& temp_slot = temp_slots[i];
        uint16_t record_size = slot_io::get_record_size(&temp_slot);
        offset -= record_size;
        slot_io::set_offset(&temp_slot, offset);
        page_io::leaf::set_slot((page_t*)leaf_page->frame, i, &temp_slot);
        page_io::leaf::set_record((page_t*)leaf_page->frame, offset, temp_records[i].c_str(), record_size);
        page_io::leaf::update_free_space((page_t*)leaf_page->frame, record_size + SLOT_SIZE);
    }
    page_io::set_key_count((page_t*)leaf_page->frame, split);

    pagenum_t new_leaf = make_leaf(table_id);
    buffer_t* new_leaf_page = buffer_manager.buffer_read_page(table_id, new_leaf);

    buffer_manager.buffer_write_page(table_id, new_leaf);
    offset = PAGE_SIZE;
    for(slotnum_t i = split, j = 0; i < num_keys + 1; ++i, ++j) {
        slot_t& temp_slot = temp_slots[i];
        uint16_t record_size = slot_io::get_record_size(&temp_slot);
        offset -= record_size;
        slot_io::set_offset(&temp_slot, offset);
        page_io::leaf::set_slot((page_t*)new_leaf_page->frame, j, &temp_slot);
        page_io::leaf::set_record((page_t*)new_leaf_page->frame, offset, temp_records[i].c_str(), record_size);
        page_io::leaf::update_free_space((page_t*)new_leaf_page->frame, record_size + SLOT_SIZE);
    }
    page_io::set_key_count((page_t*)new_leaf_page->frame, num_keys + 1 - split);

    /* Set right sibling */
    pagenum_t right_sibling = page_io::leaf::get_right_sibling((page_t*)leaf_page->frame);
    page_io::leaf::set_right_sibling((page_t*)leaf_page->frame, new_leaf);
    page_io::leaf::set_right_sibling((page_t*)new_leaf_page->frame, right_sibling);

    /* Set parent */
    pagenum_t parent = page_io::get_parent_page((page_t*)leaf_page->frame);
    page_io::set_parent_page((page_t*)new_leaf_page->frame, parent);

    int64_t new_key = page_io::leaf::get_key((page_t*)new_leaf_page->frame, 0);

    buffer_manager.unpin_buffer(table_id, leaf);
    buffer_manager.unpin_buffer(table_id, new_leaf);

    return insert_into_parent(table_id, root, leaf, new_key, new_leaf);
}

pagenum_t insert_into_node_after_splitting(int64_t table_id, pagenum_t root, pagenum_t old_node, pagenum_t left_index, int64_t key, pagenum_t right) {
    buffer_t* old_node_page = buffer_manager.buffer_read_page(table_id, old_node);
    uint32_t num_keys = page_io::get_key_count((page_t*)old_node_page->frame);

    std::vector<int64_t> temp_keys(num_keys + 1);
    std::vector<pagenum_t> temp_childs(num_keys + 2);

    for(pagenum_t i = 0, j = 0; i < num_keys + 1; ++i, ++j) {
        if(j == left_index + 1) j++;
        temp_childs[j] = page_io::internal::get_child((page_t*)old_node_page->frame, i);
    }

    for(pagenum_t i = 0, j = 0; i < num_keys; ++i, ++j) {
        if(j == left_index) j++;
        temp_keys[j] = page_io::internal::get_key((page_t*)old_node_page->frame, i);
    }

    temp_childs[left_index + 1] = right;
    temp_keys[left_index] = key;

    slotnum_t split = cut_internal();
    pagenum_t new_node = make_internal_node(table_id);
    buffer_t* new_node_page = buffer_manager.buffer_read_page(table_id, new_node);

    buffer_manager.buffer_write_page(table_id, old_node);
    pagenum_t i, j;
    for(i = 0; i < split - 1; ++i) {
        page_io::internal::set_child((page_t*)old_node_page->frame, i, temp_childs[i]);
        page_io::internal::set_key((page_t*)old_node_page->frame, i, temp_keys[i]);
    }
    page_io::internal::set_child((page_t*)old_node_page->frame, i, temp_childs[i]);
    page_io::set_key_count((page_t*)old_node_page->frame, split - 1);

    buffer_manager.buffer_write_page(table_id, new_node);
    int64_t prime_key = temp_keys[split - 1];
    for(++i, j = 0; i < INTERNAL_ORDER; ++i, ++j) {
        page_io::internal::set_child((page_t*)new_node_page->frame, j, temp_childs[i]);
        page_io::internal::set_key((page_t*)new_node_page->frame, j, temp_keys[i]);
    }
    page_io::internal::set_child((page_t*)new_node_page->frame, j, temp_childs[i]);
    page_io::set_key_count((page_t*)new_node_page->frame, INTERNAL_ORDER - split);

    pagenum_t parent = page_io::get_parent_page((page_t*)old_node_page->frame);
    page_io::set_parent_page((page_t*)new_node_page->frame, parent);
    num_keys = page_io::get_key_count((page_t*)new_node_page->frame);

    for(pagenum_t i = 0; i <= num_keys; ++i) {
        pagenum_t child = page_io::internal::get_child((page_t*)new_node_page->frame, i);
        buffer_t* child_page = buffer_manager.buffer_read_page(table_id, child);
        buffer_manager.buffer_write_page(table_id, child);
        page_io::set_parent_page((page_t*)child_page->frame, new_node);
        buffer_manager.unpin_buffer(table_id, child);
    }

    buffer_manager.unpin_buffer(table_id, old_node);
    buffer_manager.unpin_buffer(table_id, new_node);

    return insert_into_parent(table_id, root, old_node, prime_key, new_node);
}

pagenum_t insert_into_new_root(int64_t table_id, pagenum_t left, int64_t key, pagenum_t right) {
    pagenum_t root = make_internal_node(table_id);

    buffer_t* header_page = buffer_manager.buffer_read_page(table_id, 0);
    buffer_manager.buffer_write_page(table_id, 0);
    page_io::header::set_root_page((page_t*)header_page->frame, root);

    buffer_t* root_page = buffer_manager.buffer_read_page(table_id, root);

    page_io::internal::set_key((page_t*)root_page->frame, 0, key);
    page_io::internal::set_child((page_t*)root_page->frame, 0, left);
    page_io::internal::set_child((page_t*)root_page->frame, 1, right);
    page_io::set_key_count((page_t*)root_page->frame, 1);

    buffer_t* left_page = buffer_manager.buffer_read_page(table_id, left);
    buffer_manager.buffer_write_page(table_id, left);
    page_io::set_parent_page((page_t*)left_page->frame, root);

    buffer_t* right_page = buffer_manager.buffer_read_page(table_id, right);
    buffer_manager.buffer_write_page(table_id, right);
    page_io::set_parent_page((page_t*)right_page->frame, root);

    buffer_manager.unpin_buffer(table_id, 0);
    buffer_manager.unpin_buffer(table_id, root);
    buffer_manager.unpin_buffer(table_id, left);
    buffer_manager.unpin_buffer(table_id, right);

    return root;
}

pagenum_t insert_into_parent(int64_t table_id, pagenum_t root, pagenum_t left, int64_t key, pagenum_t right) {
    buffer_t* left_page = buffer_manager.buffer_read_page(table_id, left);
    pagenum_t parent = page_io::get_parent_page((page_t*)left_page->frame);
    buffer_manager.unpin_buffer(table_id, left);

    /* Case : New root */
    if(parent == 0) {
        return insert_into_new_root(table_id, left, key, right);
    }

    pagenum_t left_index = get_left_idx(table_id, parent, left);
    buffer_t* parent_page = buffer_manager.buffer_read_page(table_id, parent);
    uint32_t num_keys = page_io::get_key_count((page_t*)parent_page->frame);
    buffer_manager.unpin_buffer(table_id, parent);

    if(num_keys < INTERNAL_ORDER - 1) {
        return insert_into_node(table_id, root, parent, left_index, key, right);
    }

    return insert_into_node_after_splitting(table_id, root, parent, left_index, key, right);
}

pagenum_t insert_into_node(int64_t table_id, pagenum_t root, pagenum_t node, slotnum_t left_idx, int64_t key, pagenum_t right) {
    buffer_t* node_page = buffer_manager.buffer_read_page(table_id, node);
    uint32_t num_keys = page_io::get_key_count((page_t*)node_page->frame);

    buffer_manager.buffer_write_page(table_id, node);
    for(pagenum_t i = num_keys; i > left_idx; --i) {
        pagenum_t temp_child = page_io::internal::get_child((page_t*)node_page->frame, i);
        page_io::internal::set_child((page_t*)node_page->frame, i + 1, temp_child);
        int64_t temp_key = page_io::internal::get_key((page_t*)node_page->frame, i - 1);
        page_io::internal::set_key((page_t*)node_page->frame, i, temp_key);
    }

    page_io::internal::set_child((page_t*)node_page->frame, left_idx + 1, right);
    page_io::internal::set_key((page_t*)node_page->frame, left_idx, key);
    page_io::set_key_count((page_t*)node_page->frame, num_keys + 1);

    buffer_manager.unpin_buffer(table_id, node);

    return root;
}

/* Master insertion function */
pagenum_t insert(int64_t table_id, pagenum_t root, int64_t key, const char* value, uint16_t size) {
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
    buffer_t* leaf_page = buffer_manager.buffer_read_page(table_id, leaf);
    pagenum_t free_space = page_io::leaf::get_free_space((page_t*)leaf_page->frame);
    buffer_manager.unpin_buffer(table_id, leaf);

    if(free_space >= size + SLOT_SIZE) {
        return insert_into_leaf(table_id, leaf, key, value, size);
    }

    /* Case : leaf must be split. */
    return insert_into_leaf_after_splitting(table_id, root, leaf, key, value, size);
}

/* * * * * * * * * * * * * * DELETE * * * * * * * * * * * * * */ 

pagenum_t adjust_root(int64_t table_id, pagenum_t root) {
    pagenum_t new_root;

    buffer_t* root_page = buffer_manager.buffer_read_page(table_id, root);
    uint32_t num_keys = page_io::get_key_count((page_t*)root_page->frame);

    if(num_keys > 0) {
        buffer_manager.unpin_buffer(table_id, root);
        return root;
    }
    
    bool is_leaf = page_io::is_leaf((page_t*)root_page->frame);
    if(!is_leaf) {
        new_root = page_io::internal::get_child((page_t*)root_page->frame, 0);

        buffer_t* new_root_page = buffer_manager.buffer_read_page(table_id, new_root);
        buffer_manager.buffer_write_page(table_id, new_root);
        page_io::set_parent_page((page_t*)new_root_page->frame, 0);
        buffer_manager.unpin_buffer(table_id, new_root);

        buffer_t* header_page = buffer_manager.buffer_read_page(table_id, 0);
        buffer_manager.buffer_write_page(table_id, 0);
        page_io::header::set_root_page((page_t*)header_page->frame, new_root);
        buffer_manager.unpin_buffer(table_id, 0);
    }
    else {
        new_root = 0;

        buffer_t* header_page = buffer_manager.buffer_read_page(table_id, 0);
        page_io::header::set_root_page((page_t*)header_page->frame, 0);
        buffer_manager.unpin_buffer(table_id, 0);
    }

    buffer_manager.unpin_buffer(table_id, root);
    buffer_manager.buffer_free_page(table_id, root);
    
    return new_root;
}

pagenum_t remove_entry_from_node(int64_t table_id, int64_t key, pagenum_t node) {
    /* Remove the key and record(if node is leaf) from the node. */
    buffer_t* node_page = buffer_manager.buffer_read_page(table_id, node);
    bool is_leaf = page_io::is_leaf((page_t*)node_page->frame);

    /* Case : Internal node. */
    if(!is_leaf) {
        pagenum_t i = 0;
        while(true) {
            int64_t temp_key = page_io::internal::get_key((page_t*)node_page->frame, i);
            if(temp_key == key) break;
            i++;
        }

        buffer_manager.buffer_write_page(table_id, node);
        pagenum_t j = i;
        pagenum_t num_keys = page_io::get_key_count((page_t*)node_page->frame);
        for(++i; i < num_keys; ++i) {
            int64_t temp_key = page_io::internal::get_key((page_t*)node_page->frame, i);
            page_io::internal::set_key((page_t*)node_page->frame, i - 1, temp_key);
        }

        for(++j; j < num_keys + 1; ++j) {
            pagenum_t temp_child = page_io::internal::get_child((page_t*)node_page->frame, j);
            page_io::internal::set_child((page_t*)node_page->frame, j - 1, temp_child);
        }

        num_keys = page_io::get_key_count((page_t*)node_page->frame);
        page_io::set_key_count((page_t*)node_page->frame, num_keys - 1);

        buffer_manager.unpin_buffer(table_id, node);
    }
    /* Case : leaf node */
    else {
        pagenum_t i = 0;
        while(true) {
            int64_t temp_key = page_io::leaf::get_key((page_t*)node_page->frame, i);
            if(temp_key == key) break;
            i++;
        }

        buffer_manager.buffer_write_page(table_id, node);
        slotnum_t offset = (i == 0) ? PAGE_SIZE : page_io::leaf::get_offset((page_t*)node_page->frame, i - 1); 
        pagenum_t num_keys = page_io::get_key_count((page_t*)node_page->frame);
        for(++i; i < num_keys; ++i) {            
            slotnum_t temp_offset = page_io::leaf::get_offset((page_t*)node_page->frame, i);
            slotnum_t size = page_io::leaf::get_record_size((page_t*)node_page->frame, i);

            char* temp_record = new char[size];
            page_io::leaf::get_record((page_t*)node_page->frame, temp_offset, temp_record, size);
    
            slot_t temp_slot;
            slot_io::read_slot((page_t*)node_page->frame, i, &temp_slot);
            offset -= size;
            slot_io::set_offset(&temp_slot, offset);
            page_io::leaf::set_slot((page_t*)node_page->frame, i - 1, &temp_slot);
            
            page_io::leaf::set_record((page_t*)node_page->frame, offset, temp_record, size);
            delete[] temp_record;
        }

        num_keys = page_io::get_key_count((page_t*)node_page->frame);
        page_io::set_key_count((page_t*)node_page->frame, num_keys - 1);

        buffer_manager.unpin_buffer(table_id, node);
    }

    return node;    
}

pagenum_t redistribute_nodes(int64_t table_id, pagenum_t root, pagenum_t node, pagenum_t neighbor, pagenum_t neighbor_idx, pagenum_t prime_key_idx, int64_t prime_key) {
    buffer_t* neighbor_page;
    buffer_t* node_page = buffer_manager.buffer_read_page(table_id, node);
    bool is_leaf = page_io::is_leaf((page_t*)node_page->frame);
    
    /* neighbor is not on the extreme */
    if(neighbor_idx != -1) {
        buffer_manager.buffer_write_page(table_id, node);
        pagenum_t num_keys = page_io::get_key_count((page_t*)node_page->frame);
        if(!is_leaf) {
            pagenum_t temp_child = page_io::internal::get_child((page_t*)node_page->frame, num_keys);
            page_io::internal::set_child((page_t*)node_page->frame, num_keys + 1, temp_child);
            for(pagenum_t i = num_keys; i > 0; --i) {
                int64_t temp_key = page_io::internal::get_key((page_t*)node_page->frame, i - 1);
                page_io::internal::set_key((page_t*)node_page->frame, i, temp_key);
                pagenum_t child = page_io::internal::get_child((page_t*)node_page->frame, i - 1);
                page_io::internal::set_child((page_t*)node_page->frame, i, child);
            }

            neighbor_page = buffer_manager.buffer_read_page(table_id, neighbor);
            pagenum_t neighbor_num_keys = page_io::get_key_count((page_t*)neighbor_page->frame);
            pagenum_t child = page_io::internal::get_child((page_t*)neighbor_page->frame, neighbor_num_keys);
            page_io::internal::set_child((page_t*)node_page, 0, child);

            temp_child = page_io::internal::get_child((page_t*)node_page->frame, 0);

            buffer_t* temp_child_page = buffer_manager.buffer_read_page(table_id, temp_child);
            buffer_manager.buffer_write_page(table_id, temp_child);
            page_io::set_parent_page((page_t*)temp_child_page->frame, node);
            buffer_manager.unpin_buffer(table_id, temp_child);

            page_io::internal::set_key((page_t*)node_page->frame, 0, prime_key);

            pagenum_t parent = page_io::get_parent_page((page_t*)node_page->frame);
            buffer_t* parent_page = buffer_manager.buffer_read_page(table_id, parent);
            int64_t next_key = page_io::internal::get_key((page_t*)neighbor_page->frame, neighbor_num_keys - 1);

            buffer_manager.buffer_write_page(table_id, parent);
            page_io::internal::set_key((page_t*)parent_page->frame, prime_key_idx, next_key);
            buffer_manager.unpin_buffer(table_id, parent);
        }
        else {
            std::vector<comparison_struct> slots(num_keys + 1);
            for(pagenum_t i = 0; i < num_keys; ++i) {
                slot_io::read_slot((page_t*)node_page->frame, i, &slots[i + 1].slot);
                slotnum_t offset = slot_io::get_offset(&slots[i + 1].slot);
                slotnum_t cur_size = slot_io::get_record_size(&slots[i + 1].slot);
                char* temp_record = new char[cur_size];
                page_io::leaf::get_record((page_t*)node_page->frame, offset, temp_record, cur_size);
                slots[i + 1].record = std::string(temp_record, cur_size);
                delete[] temp_record;
            }
            neighbor_page = buffer_manager.buffer_read_page(table_id, neighbor);
            slot_io::read_slot((page_t*)neighbor_page->frame, num_keys, &slots[0].slot);
            slotnum_t offset = slot_io::get_offset(&slots[0].slot);
            slotnum_t cur_size = slot_io::get_record_size(&slots[0].slot);
            char* temp_record = new char[cur_size];
            page_io::leaf::get_record((page_t*)neighbor_page->frame, offset, temp_record, cur_size);
            slots[0].record = std::string(temp_record, cur_size);
            delete[] temp_record;

            buffer_manager.buffer_write_page(table_id, neighbor);
            page_io::leaf::update_free_space((page_t*)neighbor_page->frame, -(cur_size + SLOT_SIZE));
            page_io::leaf::set_free_space((page_t*)node_page->frame, INITIAL_FREE_SPACE);
            offset = PAGE_SIZE;
            for(pagenum_t i = 0; i < num_keys + 1; ++i) {
                offset -= slot_io::get_record_size(&slots[i].slot);
                slot_io::set_offset(&slots[i].slot, offset);
                page_io::leaf::set_slot((page_t*)node_page->frame, i, &slots[i].slot);
                page_io::leaf::set_record((page_t*)node_page->frame, offset, slots[i].record.c_str(), slots[i].record.size());
                page_io::leaf::update_free_space((page_t*)node_page->frame, slots[i].record.size() + SLOT_SIZE);
            }

            pagenum_t parent = page_io::get_parent_page((page_t*)node_page->frame);
            buffer_t* parent_page = buffer_manager.buffer_read_page(table_id, parent);
            int64_t next_key = page_io::leaf::get_key((page_t*)node_page->frame, 0);
            buffer_manager.buffer_write_page(table_id, parent);
            page_io::internal::set_key((page_t*)parent_page, prime_key_idx, next_key);
            buffer_manager.unpin_buffer(table_id, parent);
        }
    }
    /* neighbor is on the extreme */
    else {
        pagenum_t num_keys = page_io::get_key_count((page_t*)node_page->frame);
        if(!is_leaf) {
            buffer_manager.buffer_write_page(table_id, node);
            page_io::internal::set_key((page_t*)node_page->frame, num_keys, prime_key);
            
            neighbor_page = buffer_manager.buffer_read_page(table_id, neighbor);
            pagenum_t child = page_io::internal::get_child((page_t*)neighbor_page->frame, 0);
            page_io::internal::set_child((page_t*)node_page->frame, num_keys + 1, child);

            buffer_t* child_page = buffer_manager.buffer_read_page(table_id, child);
            buffer_manager.buffer_write_page(table_id, child);
            page_io::set_parent_page((page_t*)child_page->frame, node);
            buffer_manager.unpin_buffer(table_id, child);

            pagenum_t parent = page_io::get_parent_page((page_t*)node_page->frame);
            buffer_t* parent_page = buffer_manager.buffer_read_page(table_id, parent);
            int64_t next_key = page_io::internal::get_key((page_t*)neighbor_page->frame, 0);
            
            buffer_manager.buffer_write_page(table_id, parent);
            page_io::internal::set_key((page_t*)parent_page->frame, prime_key_idx, next_key);
            buffer_manager.unpin_buffer(table_id, parent);

            /* Shift key and child */
            pagenum_t neighbor_num_keys = page_io::get_key_count((page_t*)neighbor_page->frame);
            pagenum_t i;
            for(i = 0; i < neighbor_num_keys - 1; ++i) {
                int64_t temp_key = page_io::internal::get_key((page_t*)neighbor_page->frame, i + 1);
                page_io::internal::set_key((page_t*)neighbor_page->frame, i, temp_key);
                pagenum_t temp_child = page_io::internal::get_child((page_t*)neighbor_page->frame, i + 1);
                page_io::internal::set_child((page_t*)neighbor_page->frame, i, temp_child);
            }
            pagenum_t temp_child = page_io::internal::get_child((page_t*)neighbor_page->frame, i + 1);
            page_io::internal::set_child((page_t*)neighbor_page->frame, i, temp_child);
        }   
        else {
            slotnum_t offset = page_io::leaf::get_offset((page_t*)node_page->frame, num_keys - 1);

            neighbor_page = buffer_manager.buffer_read_page(table_id, neighbor);
            
            slot_t neighbor_slot;
            slot_io::read_slot((page_t*)neighbor_page->frame, 0, &neighbor_slot);
            slotnum_t neighbor_offset = slot_io::get_offset(&neighbor_slot);
            slotnum_t neighbor_size = slot_io::get_record_size(&neighbor_slot);
            char* temp_record = new char[neighbor_size];
            page_io::leaf::get_record((page_t*)neighbor_page->frame, neighbor_offset, temp_record, neighbor_size);

            buffer_manager.buffer_write_page(table_id, node);
            slot_io::set_offset(&neighbor_slot, offset - neighbor_size);
            page_io::leaf::set_slot((page_t*)node_page->frame, num_keys, &neighbor_slot);
            page_io::leaf::set_record((page_t*)node_page->frame, offset - neighbor_size, temp_record, neighbor_size);
            delete[] temp_record;

            buffer_manager.buffer_write_page(table_id, neighbor);
            page_io::leaf::update_free_space((page_t*)node_page->frame, neighbor_size + SLOT_SIZE);
            page_io::leaf::update_free_space((page_t*)neighbor_page->frame, -(neighbor_size + SLOT_SIZE));

            pagenum_t parent = page_io::get_parent_page((page_t*)node_page->frame);

            buffer_t* parent_page = buffer_manager.buffer_read_page(table_id, parent);
            int64_t next_key = page_io::leaf::get_key((page_t*)neighbor_page->frame, 1);
            buffer_manager.buffer_write_page(table_id, parent);
            page_io::internal::set_key((page_t*)parent_page->frame, prime_key_idx, next_key);
            buffer_manager.unpin_buffer(table_id, parent);

            /* Shift key and child */
            std::vector<comparison_struct> slots;
            pagenum_t neighbor_num_keys = page_io::get_key_count((page_t*)neighbor_page->frame);
            for(pagenum_t i = 0; i < neighbor_num_keys - 1; ++i) {
                comparison_struct temp;
                slot_io::read_slot((page_t*)neighbor_page->frame, i + 1, &temp.slot);
                temp_record = new char[slot_io::get_record_size(&temp.slot)];
                offset = slot_io::get_offset(&temp.slot);
                page_io::leaf::get_record((page_t*)neighbor_page->frame, offset, temp_record, slot_io::get_record_size(&temp.slot));
                temp.record = std::string(temp_record, slot_io::get_record_size(&temp.slot));
                slots.push_back(temp);
                delete[] temp_record;
            }

            page_io::leaf::set_free_space((page_t*)neighbor_page->frame, INITIAL_FREE_SPACE);
            offset = PAGE_SIZE;
            for(pagenum_t i = 0; i < neighbor_num_keys - 1; ++i) {
                offset -= slot_io::get_record_size(&slots[i].slot);
                slot_io::set_offset(&slots[i].slot, offset);
                page_io::leaf::set_slot((page_t*)neighbor_page->frame, i, &slots[i].slot);
                page_io::leaf::set_record((page_t*)neighbor_page->frame, offset, slots[i].record.c_str(), slots[i].record.size());
                page_io::leaf::update_free_space((page_t*)neighbor_page->frame, slot_io::get_record_size(&slots[i].slot) + SLOT_SIZE);
            }
        }
    }

    pagenum_t node_num_keys = page_io::get_key_count((page_t*)node_page->frame);
    pagenum_t neighbor_num_keys = page_io::get_key_count((page_t*)neighbor_page->frame);
    page_io::set_key_count((page_t*)node_page->frame, node_num_keys + 1);
    page_io::set_key_count((page_t*)neighbor_page->frame, neighbor_num_keys - 1);

    buffer_manager.unpin_buffer(table_id, neighbor);
    buffer_manager.unpin_buffer(table_id, node);

    return root;
}

pagenum_t merge_nodes(int64_t table_id, pagenum_t root, pagenum_t node, pagenum_t neighbor, pagenum_t neighbor_idx, int64_t prime_key) {
    if(neighbor_idx == -1) std::swap(node, neighbor);

    buffer_t* neighbor_page = buffer_manager.buffer_read_page(table_id, neighbor);
    pagenum_t neighbor_insertion_idx = page_io::get_key_count((page_t*)neighbor_page->frame);

    buffer_t* node_page = buffer_manager.buffer_read_page(table_id, node);
    bool is_leaf = page_io::is_leaf((page_t*)node_page->frame);

    if(!is_leaf) {
        pagenum_t neighbor_num_keys, node_num_keys;

        buffer_manager.buffer_write_page(table_id, neighbor);
        page_io::internal::set_key((page_t*)neighbor_page->frame, neighbor_insertion_idx, prime_key);
        neighbor_num_keys = page_io::get_key_count((page_t*)neighbor_page->frame);
        page_io::set_key_count((page_t*)neighbor_page->frame, neighbor_num_keys + 1);
        
        buffer_manager.buffer_write_page(table_id, node);
        pagenum_t i, j;
        pagenum_t node_end = page_io::get_key_count((page_t*)node_page->frame);
        for(i = neighbor_insertion_idx + 1, j = 0; j < node_end; ++i, ++j) {
            pagenum_t temp_child = page_io::internal::get_child((page_t*)node_page->frame, j);
            page_io::internal::set_child((page_t*)neighbor_page->frame, i, temp_child);
            int64_t temp_key = page_io::internal::get_key((page_t*)node_page->frame, j);
            page_io::internal::set_key((page_t*)neighbor_page->frame, i, temp_key);

            /* update key count */
            neighbor_num_keys = page_io::get_key_count((page_t*)neighbor_page->frame);
            node_num_keys = page_io::get_key_count((page_t*)node_page->frame);
            page_io::set_key_count((page_t*)neighbor_page->frame, neighbor_num_keys + 1);
            page_io::set_key_count((page_t*)node_page->frame, node_num_keys - 1);
        }
        pagenum_t temp_child = page_io::internal::get_child((page_t*)node_page->frame, j);
        page_io::internal::set_child((page_t*)neighbor_page->frame, i, temp_child);

        neighbor_num_keys = page_io::get_key_count((page_t*)neighbor_page->frame);
        for(pagenum_t i = 0; i < neighbor_num_keys + 1; ++i) {
            pagenum_t child = page_io::internal::get_child((page_t*)neighbor_page->frame, i);
            
            buffer_t* child_page = buffer_manager.buffer_read_page(table_id, child);
            buffer_manager.buffer_write_page(table_id, child);
            page_io::set_parent_page((page_t*)child_page->frame, neighbor);
            buffer_manager.unpin_buffer(table_id, child);
        }

        buffer_manager.unpin_buffer(table_id, neighbor);  
    }
    else {
        /* Leaf node merge */
        pagenum_t num_keys = page_io::get_key_count((page_t*)node_page->frame);
        slotnum_t offset = page_io::leaf::get_offset((page_t*)neighbor_page->frame, neighbor_insertion_idx - 1);
        
        buffer_manager.buffer_write_page(table_id, neighbor);
        buffer_manager.buffer_write_page(table_id, node);
        for(pagenum_t i = neighbor_insertion_idx, j = 0; j < num_keys; ++i, ++j) {
            slot_t temp_slot;
            slot_io::read_slot((page_t*)node_page->frame, j, &temp_slot);
            slotnum_t size = slot_io::get_record_size(&temp_slot);
            offset -= size;
            slot_io::set_offset(&temp_slot, offset);
            page_io::leaf::set_slot((page_t*)neighbor_page->frame, i, &temp_slot);

            char* temp_record = new char[size];
            slotnum_t temp_offset = slot_io::get_offset(&temp_slot);
            page_io::leaf::get_record((page_t*)node_page->frame, temp_offset, temp_record, size);
            page_io::leaf::set_record((page_t*)neighbor_page->frame, offset, temp_record, size);
            delete[] temp_record;

            /* update key count */
            pagenum_t neighbor_num_keys = page_io::get_key_count((page_t*)neighbor_page->frame);
            page_io::set_key_count((page_t*)neighbor_page->frame, neighbor_num_keys + 1);
            page_io::leaf::update_free_space((page_t*)neighbor_page->frame, size + SLOT_SIZE);
        }

        pagenum_t right_sibling = page_io::leaf::get_right_sibling((page_t*)node_page->frame);
        page_io::leaf::set_right_sibling((page_t*)neighbor_page->frame, right_sibling);

        buffer_manager.unpin_buffer(table_id, neighbor);
    }

    pagenum_t parent = page_io::get_parent_page((page_t*)node_page->frame);
    buffer_manager.unpin_buffer(table_id, node);

    root = delete_entry(table_id, root, parent, prime_key);
    buffer_manager.buffer_free_page(table_id, node);

    return root;
}

pagenum_t delete_entry(int64_t table_id, pagenum_t root, pagenum_t node, int64_t key) {
    node = remove_entry_from_node(table_id, key, node);

    /* Case : Deletion from the root */
    if(node == root) return adjust_root(table_id, root);

    buffer_t* node_page = buffer_manager.buffer_read_page(table_id, node);
    uint32_t num_keys = page_io::get_key_count((page_t*)node_page->frame);
    bool is_leaf = page_io::is_leaf((page_t*)node_page->frame);

    int32_t min_keys = is_leaf ? -1 : cut_internal();
    
    /* Case : node stays at or above minimum after deletion */
    if(!is_leaf && num_keys >= min_keys) {
        buffer_manager.unpin_buffer(table_id, node);
        return root;
    }
    else if(is_leaf) {
        pagenum_t free_space = page_io::leaf::get_free_space((page_t*)node_page->frame);
        if(free_space < THRESHOLD) {
            buffer_manager.unpin_buffer(table_id, node);
            return root;
        }
    }

    pagenum_t neighbor_idx = get_neighbor_idx(table_id, node);
    pagenum_t prime_key_idx = (neighbor_idx == -1) ? 0 : neighbor_idx;
    int64_t prime_key = is_leaf 
    ? page_io::leaf::get_key((page_t*)node_page->frame, prime_key_idx) 
    : page_io::internal::get_key((page_t*)node_page->frame, prime_key_idx);
    
    pagenum_t parent = page_io::get_parent_page((page_t*)node_page->frame);
    buffer_t* parent_page = buffer_manager.buffer_read_page(table_id, parent);

    pagenum_t neighbor = (neighbor_idx == -1) 
    ? page_io::internal::get_child((page_t*)parent_page->frame, 1) 
    : page_io::internal::get_child((page_t*)parent_page->frame, neighbor_idx);

    buffer_t* neighbor_page = buffer_manager.buffer_read_page(table_id, neighbor);

    uint32_t neighbor_num_keys = page_io::get_key_count((page_t*)neighbor_page->frame);

    if(!is_leaf) {
        buffer_manager.unpin_buffer(table_id, node);
        buffer_manager.unpin_buffer(table_id, neighbor);
        buffer_manager.unpin_buffer(table_id, parent);
    
        if(neighbor_num_keys + num_keys < INTERNAL_ORDER - 1) return merge_nodes(table_id, root, node, neighbor, neighbor_idx, prime_key);
        else return redistribute_nodes(table_id, root, node, neighbor, neighbor_idx, prime_key_idx, prime_key);
    }
    else {
        pagenum_t free_space = page_io::leaf::get_free_space((page_t*)node_page->frame);
        pagenum_t neighbor_free_space = page_io::leaf::get_free_space((page_t*)neighbor_page->frame);
        pagenum_t merged_space = (PAGE_SIZE - PAGE_HEADER_SIZE) - free_space
        + (PAGE_SIZE - PAGE_HEADER_SIZE) - neighbor_free_space;

        if(merged_space <= (PAGE_SIZE - PAGE_HEADER_SIZE)) {
            buffer_manager.unpin_buffer(table_id, node);
            buffer_manager.unpin_buffer(table_id, neighbor);
            buffer_manager.unpin_buffer(table_id, parent);
            return merge_nodes(table_id, root, node, neighbor, neighbor_idx, prime_key);
        }
        else {
            do {
                neighbor_idx = get_neighbor_idx(table_id, node);
                prime_key_idx = (neighbor_idx == -1) ? 0 : neighbor_idx;
                prime_key = page_io::leaf::get_key((page_t*)node_page->frame, prime_key_idx);

                root = redistribute_nodes(table_id, root, node, neighbor, neighbor_idx, prime_key_idx, prime_key);

                node_page = buffer_manager.buffer_read_page(table_id, node);                
            }
            while(page_io::leaf::get_free_space((page_t*)node_page->frame) >= THRESHOLD);

            buffer_manager.unpin_buffer(table_id, node);
            buffer_manager.unpin_buffer(table_id, neighbor);
            buffer_manager.unpin_buffer(table_id, parent);

            return root;
        }
    }
}

/* Master deletion function */
pagenum_t master_delete(int64_t table_id, pagenum_t root, int64_t key) {
    pagenum_t key_leaf = find_leaf(table_id, root, key);
    auto location_pair = find(table_id, root, key);

    if(location_pair != std::pair<pagenum_t, slotnum_t>({0, 0})) 
        root = delete_entry(table_id, root, key_leaf, key);

    return root;
}
