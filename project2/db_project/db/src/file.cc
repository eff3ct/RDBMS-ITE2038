#include "file.h"

std::vector<int> opened_files;

/* Collection of I/O functions for DB space manager */
namespace db_io {
    /* Collection of 'Page I/O' functions */
    namespace page_io {
        // Read a page(pagenum) from file(fd) into buffer(dest)
        void read_page(int fd, pagenum_t pagenum, void* dest) {
            pread(fd, dest, PAGE_SIZE, pagenum * PAGE_SIZE);
        }
        // Write a src to page(pagenum) of file(fd)
        void write_page(int fd, pagenum_t pagenum, const page_t* src) {
            pwrite(fd, src->data, PAGE_SIZE, pagenum * PAGE_SIZE);
            sync();
        }
        /* 
        Set header page of file(fd), 
        [0] = magic number(2022), 
        [1] = next page number(next_free_page), 
        [2] = number of pages(page_cnt)
        */
        void set_header_page(int fd, pagenum_t next_free_page, pagenum_t page_cnt) {
            page_t header_page;

            pagenum_t magic_number = MAGIC_NUMBER;

            memcpy(header_page.data, &magic_number, sizeof(pagenum_t));
            memcpy(header_page.data + sizeof(pagenum_t), &next_free_page, sizeof(pagenum_t));
            memcpy(header_page.data + sizeof(pagenum_t) * 2, &page_cnt, sizeof(pagenum_t));

            write_page(fd, 0, &header_page);
        }
        // Make more free pages(new_page_cnt) of file(fd) from (start_page) where total number of pages is (total_cnt)
        void make_free_pages(int fd, pagenum_t start_pagenum, pagenum_t new_page_cnt, pagenum_t total_cnt) {
            page_t free_page;
            for(pagenum_t i = start_pagenum; i < start_pagenum + new_page_cnt; i++) {
                pagenum_t next_free_page = (i + 1) % total_cnt;
                memcpy(free_page.data, &next_free_page, sizeof(pagenum_t));
                write_page(fd, i, &free_page);
            }
        }
        // Get next free page number of file(fd) from page(pagenum)
        pagenum_t get_next_free_page(int fd, pagenum_t pagenum) {
            pagenum_t* buf = (pagenum_t*)malloc(PAGE_SIZE);
            int idx = (pagenum == 0);
            read_page(fd, pagenum, buf);
            pagenum_t next_free_page = buf[idx];
            free(buf);
            return next_free_page;
        }
        // Get total page count from header page where file is (fd).
        pagenum_t get_page_count(int fd) {
            pagenum_t* buf = (pagenum_t*)malloc(PAGE_SIZE);
            read_page(fd, 0, buf);
            pagenum_t page_cnt = buf[2];
            free(buf);
            return page_cnt;
        }
    }
    /* Collection of 'File I/O' Functions */
    namespace file_io {
        // Check validity of the magic number of a current file(fd)
        bool is_valid_magic_number(int fd) {
            pagenum_t* buf = (pagenum_t*)malloc(PAGE_SIZE);
            page_io::read_page(fd, 0, buf);
            pagenum_t magic_number = buf[0];
            return magic_number == MAGIC_NUMBER;
            free(buf);
        }
        // Doubling size of the file(fd)
        void extend_file(int fd) {
            pagenum_t num_pages = page_io::get_page_count(fd);
            page_io::set_header_page(fd, num_pages, 2 * num_pages);
            page_io::make_free_pages(fd, num_pages, num_pages, 2 * num_pages);
        }
    }
}

// Open existing database file or create one if it doesn't exist
int file_open_database_file(const char* pathname) {
    int fd = open(pathname, O_RDWR | O_SYNC);
    // If file doesn't exist, create one
    if(fd < 0) {
        fd = open(pathname, O_RDWR | O_CREAT | O_SYNC, 0644);
        pagenum_t page_cnt = INITIAL_DB_FILE_SIZE / PAGE_SIZE;
        db_io::page_io::set_header_page(fd, 1, page_cnt);
        db_io::page_io::make_free_pages(fd, 1, page_cnt - 1, page_cnt);
    }

    if(!db_io::file_io::is_valid_magic_number(fd)) return -1;

    opened_files.push_back(fd);

    return fd;
}

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int fd) {
    pagenum_t next_free_page = db_io::page_io::get_next_free_page(fd, 0);

    if(next_free_page == 0) {
        db_io::file_io::extend_file(fd);
        next_free_page = db_io::page_io::get_next_free_page(fd, 0);
    }

    pagenum_t new_next_free_page;
    new_next_free_page = db_io::page_io::get_next_free_page(fd, next_free_page);

    pagenum_t page_cnt = db_io::page_io::get_page_count(fd);
    db_io::page_io::set_header_page(fd, new_next_free_page, page_cnt);

    return next_free_page;
}

// Free an on-disk page to the free page list
void file_free_page(int fd, pagenum_t pagenum) {
    pagenum_t next_free_page = db_io::page_io::get_next_free_page(fd, 0);
    pagenum_t page_cnt = db_io::page_io::get_page_count(fd);
    db_io::page_io::set_header_page(fd, pagenum, page_cnt);

    page_t freed_page;
    memcpy(freed_page.data, &next_free_page, sizeof(pagenum_t));
    db_io::page_io::write_page(fd, pagenum, &freed_page);
}

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int fd, pagenum_t pagenum, struct page_t* dest) {
    db_io::page_io::read_page(fd, pagenum, dest);
}

// Write an in-memory page(src) to the on-disk page
void file_write_page(int fd, pagenum_t pagenum, const struct page_t* src) {
    db_io::page_io::write_page(fd, pagenum, src);
}

// Close the database file
void file_close_database_file() {
    for(int& fd : opened_files) close(fd);
    opened_files.clear();
}