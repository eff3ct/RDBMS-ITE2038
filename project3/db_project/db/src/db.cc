#include "db.h"

#define MAX_TABLES 20

extern BufferManager buffer_manager;

/* Open existing data file using ‘pathname’ or create one if not existed. 
* If success, return a unique table id else return negative value.
*/
std::set<std::string> opened_file_paths;

int64_t open_table(const char* pathname) {
    std::string path(pathname);
    if(opened_file_paths.find(path) != opened_file_paths.end()) return -1;
    if(opened_file_paths.size() >= MAX_TABLES) return -1;
    opened_file_paths.insert(path);

    int64_t table_id = buffer_manager.buffer_open_table_file(pathname);
    if(table_id < 0) return -1; // open failed.
    return table_id; // open success.
}

/* Insert key/value pair to data file.
* If success, return 0 else return non-zero value.
*/
int db_insert(int64_t table_id, int64_t key, const char* value, uint16_t val_size) {
    // valid size check
    if(val_size < 50 || val_size > 112) return -1;

    buffer_t* header = buffer_manager.buffer_read_page(table_id, 0);
    pagenum_t root = page_io::header::get_root_page((page_t*)header->frame);
    buffer_manager.unpin_buffer(table_id, 0);

    insert(table_id, root, key, value, val_size);

    return 0;
}

/* Find a record containing the 'key'.
* If a matching key exists, store its value in 'ret_val' and the corresponding size in 'val_size'.
* If success, return 0 else return non-zero value.
*/
int db_find(int64_t table_id, int64_t key, char* ret_val, uint16_t* val_size) {
    buffer_t* header = buffer_manager.buffer_read_page(table_id, 0);
    pagenum_t root = page_io::header::get_root_page((page_t*)header->frame);
    buffer_manager.unpin_buffer(table_id, 0);

    auto location_pair = find(table_id, root, key);
    
    if(location_pair == std::pair<pagenum_t, slotnum_t>({0, 0})) return -1;
    
    buffer_t* page = buffer_manager.buffer_read_page(table_id, location_pair.first);
    *val_size = page_io::leaf::get_record_size((page_t*)page->frame, location_pair.second);
    slotnum_t offset = page_io::leaf::get_offset((page_t*)page->frame, location_pair.second);
    page_io::leaf::get_record((page_t*)page->frame, offset, ret_val, *val_size);
    buffer_manager.unpin_buffer(table_id, location_pair.first);

    return 0;
}

int db_delete(int64_t table_id, int64_t key) {
    buffer_t* header = buffer_manager.buffer_read_page(table_id, 0);
    pagenum_t root = page_io::header::get_root_page((page_t*)header->frame);
    buffer_manager.unpin_buffer(table_id, 0);

    master_delete(table_id, root, key);

    return 0;
}

int db_scan(int64_t table_id, int64_t begin_key, int64_t end_key,
std::vector<int64_t>* keys, std::vector<char*>* values, std::vector<uint16_t>* val_sizes) {
    buffer_t* header = buffer_manager.buffer_read_page(table_id, 0);
    pagenum_t root = page_io::header::get_root_page((page_t*)header->frame);
    buffer_manager.unpin_buffer(table_id, 0);

    pagenum_t node = find_leaf(table_id, root, begin_key);

    if(node == 0) return -1;

    buffer_t* page = buffer_manager.buffer_read_page(table_id, node);

    pagenum_t i = 0;
    uint32_t num_keys = page_io::get_key_count((page_t*)page->frame);

    while(i < num_keys 
    && page_io::leaf::get_key((page_t*)page->frame, i) < begin_key) 
        i++;

    if(i == num_keys) return -1;

    buffer_manager.unpin_buffer(table_id, node);
    while(node != 0) {
        page = buffer_manager.buffer_read_page(table_id, node);
        num_keys = page_io::get_key_count((page_t*)page->frame);
        for(; i < num_keys && page_io::leaf::get_key((page_t*)page->frame, i) <= end_key; ++i) {
            keys->push_back(page_io::leaf::get_key((page_t*)page->frame, i));
            uint16_t val_size = page_io::leaf::get_record_size((page_t*)page->frame, i);
            slotnum_t offset = page_io::leaf::get_offset((page_t*)page->frame, i);
            char* value = new char[val_size];
            page_io::leaf::get_record((page_t*)page->frame, offset, value, val_size);
            values->push_back(value);
            val_sizes->push_back(val_size);
        }
        pagenum_t new_node = page_io::leaf::get_right_sibling((page_t*)page->frame);
        buffer_manager.unpin_buffer(table_id, node);
        node = new_node;
        i = 0;
    }

    return 0;
}

int init_db(int num_buf) {
    buffer_manager.set_max_count(num_buf);
    return 0;
}

int shutdown_db() {
    buffer_manager.destroy_all();
    buffer_manager.buffer_close_table_file();
    opened_file_paths.clear();
    return 0;
}
