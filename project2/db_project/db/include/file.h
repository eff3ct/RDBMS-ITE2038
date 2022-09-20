#ifndef DB_FILE_H_
#define DB_FILE_H_

#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>

#include <vector>

#define INITIAL_DB_FILE_SIZE (10 * 1024 * 1024)  // 10 MiB
#define PAGE_SIZE (4 * 1024)                     // 4 KiB
#define MAGIC_NUMBER (2022)

typedef uint64_t pagenum_t;

extern std::vector<int> opened_files;

struct page_t {
    char data[PAGE_SIZE];
};

// Open existing database file or create one if it doesn't exist
int file_open_database_file(const char* pathname);

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int fd);

// Free an on-disk page to the free page list
void file_free_page(int fd, pagenum_t pagenum);

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int fd, pagenum_t pagenum, struct page_t* dest);

// Write an in-memory page(src) to the on-disk page
void file_write_page(int fd, pagenum_t pagenum, const struct page_t* src);

// Close the database file
void file_close_database_file();

#endif  // DB_FILE_H_
