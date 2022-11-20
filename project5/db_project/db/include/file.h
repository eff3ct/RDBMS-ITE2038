#ifndef DB_FILE_H_
#define DB_FILE_H_

#include <vector>
#include <map>
#include <utility>
#include <string>

#include "page.h"

namespace file_io {
    off_t get_file_size(int fd);
    bool is_valid_magic_number(int fd);
    void extend_file(int fd);
    void read_page(int fd, pagenum_t pagenum, void* dest);
    void write_page(int fd, pagenum_t pagenum, const page_t* src);
    void make_free_pages(int fd, pagenum_t start_pagenum, pagenum_t new_page_cnt, pagenum_t total_cnt);
}

// Manger for opened tables.
class TableManager {
    int64_t next_table_id;
    std::map<int, int64_t> fd_to_table_id;
    std::map<int64_t, int> table_id_to_fd;
    std::vector<int> opened_files;

    public:
        TableManager() : next_table_id(0) {}
        int64_t get_table_id(int fd);
        int get_fd(int64_t table_id);
        void insert_table(int fd);
        void close_all();
};

extern TableManager table_manager;

// Open existing database file or create one if it doesn't exist
int64_t file_open_table_file(const char* pathname);

// Allocate an on-disk page from the free page list
uint64_t file_alloc_page(int64_t table_id);

// Free an on-disk page to the free page list
void file_free_page(int64_t table_id, pagenum_t page_number);

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int64_t table_id, pagenum_t page_number, struct page_t* dest);

// Write an in-memory page(src) to the on-disk page
void file_write_page(int64_t table_id, pagenum_t page_number, const struct page_t* src);

// Close the database file
void file_close_table_files();

#endif  // DB_FILE_H_
