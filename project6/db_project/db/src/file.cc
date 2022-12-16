#include "file.h"

/* Table Manager Functions */

/* FILE IO */
// Get size of a file(fd) using lseek syscall 
off_t file_io::get_file_size(int fd) {
    off_t cur_position = lseek(fd, 0, SEEK_CUR);
    off_t sz = lseek(fd, 0, SEEK_END);
    lseek(fd, cur_position, SEEK_SET);
    return sz;
}
// Read a page(pagenum) of file(fd) into (dest)
void file_io::read_page(int fd, pagenum_t pagenum, void* dest) {
    pread(fd, dest, PAGE_SIZE, pagenum * PAGE_SIZE);
}
// Write a src to page(pagenum) of file(fd)
void file_io::write_page(int fd, pagenum_t pagenum, const page_t* src) {
    pwrite(fd, src->data, PAGE_SIZE, pagenum * PAGE_SIZE);
    sync();
}
// Check validity of the magic number of a current file(fd)
bool file_io::is_valid_magic_number(int fd) {
    pagenum_t* buf = (pagenum_t*)malloc(PAGE_SIZE);
    file_io::read_page(fd, 0, buf);
    pagenum_t magic_number = buf[0];
    free(buf);
    return magic_number == MAGIC_NUMBER;
}
// Doubling size of the file(fd)
void file_io::extend_file(int fd) {
    page_t header_page;
    file_io::read_page(fd, 0, &header_page);

    pagenum_t num_pages = page_io::header::get_page_count(&header_page);
    pagenum_t root_page = page_io::header::get_root_page(&header_page);

    page_io::header::set_header_page(&header_page, num_pages, 2 * num_pages, root_page);
    file_io::write_page(fd, 0, &header_page);
    file_io::make_free_pages(fd, num_pages, num_pages, 2 * num_pages);
}
// Make more free pages(new_page_cnt) of file(fd) from (start_page) where total number of pages is (total_cnt)
void file_io::make_free_pages(int fd, pagenum_t start_pagenum, pagenum_t new_page_cnt, pagenum_t total_cnt) {
    page_t free_page;
    for(pagenum_t i = start_pagenum; i < start_pagenum + new_page_cnt; i++) {
        pagenum_t next_free_page = (i + 1) % total_cnt;
        memcpy(free_page.data, &next_free_page, sizeof(pagenum_t));
        file_io::write_page(fd, i, &free_page);
    }
}

/* Table Manager */
int64_t TableManager::get_table_id(int fd) {
    return fd_to_table_id[fd];
}
/* Implemented For Project 6 */   
int64_t TableManager::get_table_id(std::string pathname) {
    int64_t table_id = std::stoi(pathname.substr(4));
    return table_id;
}
int TableManager::get_fd(int64_t table_id) {
    return table_id_to_fd[table_id];
}
/* Implemented For Project 6 */   
void TableManager::insert_table(int fd, std::string pathname) {
    int64_t table_id = std::stoi(pathname.substr(4));
    fd_to_table_id.insert({fd, table_id});
    table_id_to_fd.insert({table_id, fd});
    opened_files.push_back(fd);
}
void TableManager::insert_table(int fd) {
    fd_to_table_id.insert({fd, next_table_id});
    table_id_to_fd.insert({next_table_id, fd});
    opened_files.push_back(fd);
    next_table_id++;
}
void TableManager::close_all() {
    for(int& fd : opened_files) close(fd);
    fd_to_table_id.clear();
    table_id_to_fd.clear();
    opened_files.clear();
    next_table_id = 0;
}

/* Global Table Manager */
TableManager table_manager;

// Open existing database file or create one if it doesn't exist
int64_t file_open_table_file(const char* pathname) {
    int fd = open(pathname, O_RDWR | O_SYNC);

    // If file doesn't exist, create one
    if(fd < 0) {
        fd = open(pathname, O_RDWR | O_CREAT | O_SYNC, 0644);
        pagenum_t page_cnt = INITIAL_DB_FILE_SIZE / PAGE_SIZE;
        
        page_t header_page;
        page_io::header::set_header_page(&header_page, 1, page_cnt, 0);
        file_io::write_page(fd, 0, &header_page);

        file_io::make_free_pages(fd, 1, page_cnt - 1, page_cnt);
    }

    if(!file_io::is_valid_magic_number(fd)) return -1;

    table_manager.insert_table(fd, std::string(pathname));
    int64_t table_id = table_manager.get_table_id(std::string(pathname));

    return table_id;
}

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int64_t table_id) {
    int fd = table_manager.get_fd(table_id);

    page_t header_page;
    file_io::read_page(fd, 0, &header_page);
    pagenum_t next_free_page = page_io::get_next_free_page(&header_page, 0);

    if(next_free_page == 0) {
        file_io::extend_file(fd);
        file_io::read_page(fd, 0, &header_page);
        next_free_page = page_io::get_next_free_page(&header_page, 0);
    }

    page_t free_page;
    file_io::read_page(fd, next_free_page, &free_page);
    pagenum_t new_next_free_page = page_io::get_next_free_page(&free_page, next_free_page);

    file_io::read_page(fd, 0, &header_page);
    pagenum_t page_cnt = page_io::header::get_page_count(&header_page);
    pagenum_t root_page = page_io::header::get_root_page(&header_page);
    page_io::header::set_header_page(&header_page, new_next_free_page, page_cnt, root_page);
    file_io::write_page(fd, 0, &header_page);

    return next_free_page;
}

// Free an on-disk page to the free page list
void file_free_page(int64_t table_id, pagenum_t pagenum) {
    int fd = table_manager.get_fd(table_id);

    page_t header_page;
    file_io::read_page(fd, 0, &header_page);

    pagenum_t next_free_page = page_io::get_next_free_page(&header_page, 0);
    pagenum_t page_cnt = page_io::header::get_page_count(&header_page);
    pagenum_t root_page = page_io::header::get_root_page(&header_page);
    page_io::header::set_header_page(&header_page, pagenum, page_cnt, root_page);
    file_io::write_page(fd, 0, &header_page);

    page_t freed_page;
    memcpy(freed_page.data, &next_free_page, sizeof(pagenum_t));
    file_io::write_page(fd, pagenum, &freed_page);
}

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int64_t table_id, pagenum_t pagenum, struct page_t* dest) {
    int fd = table_manager.get_fd(table_id);
    file_io::read_page(fd, pagenum, dest);
}

// Write an in-memory page(src) to the on-disk page
void file_write_page(int64_t table_id, pagenum_t pagenum, const struct page_t* src) {
    int fd = table_manager.get_fd(table_id);
    file_io::write_page(fd, pagenum, src);
}

// Close the database file
void file_close_table_files() {
    table_manager.close_all();
}