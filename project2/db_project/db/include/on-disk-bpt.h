#ifndef ON_DISK_BPT_H
#define ON_DISK_BPT_H

#include "file.h"

/* Util Functions */
slotnum_t cut_leaf(page_t* leaf);
slotnum_t cut_internal();
pagenum_t get_left_idx(int64_t table_id, pagenum_t parent, pagenum_t left);
pagenum_t get_neighbor_idx(int64_t table_id, pagenum_t node);

/* Find */
pagenum_t find_leaf(int64_t table_id, pagenum_t root, int64_t key);
std::pair<pagenum_t, slotnum_t> find(int64_t table_id, pagenum_t root, int64_t key);

/* Insertion */
pagenum_t make_internal_node(int64_t table_id);
pagenum_t make_leaf(int64_t table_id);
pagenum_t insert_into_leaf(int64_t table_id, pagenum_t leaf, int64_t key, const char* value, uint16_t size);
pagenum_t insert_into_leaf_after_splitting(int64_t table_id, pagenum_t root, pagenum_t leaf, int64_t key, const char* value, uint16_t size);
pagenum_t insert_into_node(int64_t table_id, pagenum_t root, pagenum_t node, slotnum_t left_idx, int64_t key, pagenum_t right);
pagenum_t insert_into_node_after_splitting(int64_t table_id, pagenum_t root, pagenum_t old_node, pagenum_t left_index, int64_t key, pagenum_t right);
pagenum_t insert_into_parent(int64_t table_id, pagenum_t root, pagenum_t left, int64_t key, pagenum_t right);
pagenum_t insert_into_new_root(int64_t table_id, pagenum_t left, int64_t key, pagenum_t right);
pagenum_t start_new_tree(int64_t table_id, int64_t key, const char* value, uint16_t size);
pagenum_t insert(int64_t table_id, pagenum_t root, int64_t key, const char* value, uint16_t size);

/* Deletion */
pagenum_t adjust_root(int64_t table_id, pagenum_t root);
pagenum_t merge_nodes(int64_t table_id, pagenum_t root, pagenum_t node, pagenum_t neighbor, pagenum_t neighbor_idx, int64_t prime_key);
pagenum_t redistribute_nodes(int64_t table_id, pagenum_t root, pagenum_t node, pagenum_t neighbor, pagenum_t neighbor_idx, pagenum_t prime_key_idx, int64_t prime_key);
pagenum_t remove_entry_from_node(int64_t table_id, int64_t key, pagenum_t node);
pagenum_t delete_entry(int64_t table_id, pagenum_t root, pagenum_t node, int64_t key);
pagenum_t master_delete(int64_t table_id, pagenum_t root, int64_t key);

#endif