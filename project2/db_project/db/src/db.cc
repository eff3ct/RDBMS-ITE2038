#include "db.h"
#include "on-disk-bpt.h"
#include <iostream>

/* Open existing data file using ‘pathname’ or create one if not existed. 
* If success, return a unique table id else return negative value.
*/
int64_t open_table(const char* pathname) {
    int64_t table_id = file_open_table_file(pathname);
    if(table_id < 0) return -1; // open failed.
    return table_id; // open success.
}

/* Insert key/value pair to data file.
* If success, return 0 else return non-zero value.
*/
int db_insert(int64_t table_id, int64_t key, const char* value, uint16_t val_size) {
    // TODO
    // 1. valid data size check
    page_t header;
    file_read_page(table_id, 0, &header);
    pagenum_t root = page_io::header::get_root_page(&header);
    insert(table_id, root, key, value, val_size);

    return 0;
}

/* Find a record containing the 'key'.
* If a matching key exists, store its value in 'ret_val' and the corresponding size in 'val_size'.
* If success, return 0 else return non-zero value.
*/
int db_find(int64_t table_id, int64_t key, char* ret_val, uint16_t* val_size) {
    page_t header;
    file_read_page(table_id, 0, &header);
    pagenum_t root = page_io::header::get_root_page(&header);
    auto location_pair = find(table_id, root, key);
    
    if(location_pair == std::pair<pagenum_t, slotnum_t>({0, 0})) return -1;
    
    page_t page;
    file_read_page(table_id, location_pair.first, &page);
    *val_size = page_io::leaf::get_record_size(&page, location_pair.second);
    slotnum_t offset = page_io::leaf::get_offset(&page, location_pair.second);
    page_io::leaf::get_record(&page, offset, ret_val, *val_size);

    return 0;
}

int db_delete(int64_t table_id, int64_t key) {

}

int db_scan(int64_t table_id, int64_t begin_key, int64_t end_key,
std::vector<int64_t>* keys, std::vector<char*>* values, std::vector<uint16_t>* val_sizes) {

}

int init_db() {

}

int shutdown_db() {
    file_close_table_files();
}
