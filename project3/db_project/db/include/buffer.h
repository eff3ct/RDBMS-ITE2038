#ifndef BUFFER_H
#define BUFFER_H

#include "page.h"
#include "file.h"

#include <unordered_map>

struct buffer_t {
    char frame[PAGE_SIZE];
    int64_t table_id;
    pagenum_t pagenum;
    bool is_dirty;
    int32_t is_pinned;
    struct buffer_t* next;
    struct buffer_t* prev;

    buffer_t();
    buffer_t(int64_t table_id, pagenum_t pagenum);
};

class BufferManager {
    /* field */
    buffer_t* buf_head;
    buffer_t* buf_tail;
    std::unordered_map<int64_t, buffer_t*> hash_pointer;
    int max_count;
    int cur_count;

    private:
        /* move buffer to head */
        void move_to_head(buffer_t* buf);
        /* insert into head */
        void insert_into_head(buffer_t* buf);
        /* convert pair to key */
        int64_t convert_pair_to_key(int64_t table_id, pagenum_t pagenum);
        /* flush buffer */
        void flush_buffer(buffer_t* buf);
        /* flush (pagenum) page buffer */
        void flush_buffer(int64_t table_id, pagenum_t pagenum);
        /* check (pagenum) page buffer exist */
        bool is_buffer_exist(int64_t table_id, pagenum_t pagenum);
        /* find buffer (pagenum) page buffer location */
        buffer_t* find_buffer(int64_t table_id, pagenum_t pagenum);
        /* find victim buffer (by LRU policy) */
        buffer_t* find_victim();

    public:
        /* constructor */
        BufferManager();
        /* set max buffer count */
        void set_max_count(int max_count);
        /* unpin buffer */
        void unpin_buffer(int64_t table_id, pagenum_t pagenum);
        /* open table */
        int64_t buffer_open_table_file(const char* pathname);
        /* allocate page */
        pagenum_t buffer_alloc_page(int64_t table_id);
        /* read page through buffer */
        void buffer_read_page(int64_t table_id, pagenum_t pagenum, page_t* dest);
        /* write page through buffer */
        void buffer_write_page(int64_t table_id, pagenum_t pagenum, const page_t* src);
        /* free page */
        void buffer_free_page(int64_t table_id, pagenum_t pagenum);
        /* flush all buffer blocks and de-allocate buffer blocks */
        void destroy_all();
        /* Destructor */
        ~BufferManager();
};

extern BufferManager buffer_manager;

#endif