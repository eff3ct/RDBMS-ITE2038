#ifndef BUFFER_H
#define BUFFER_H

#include "page.h"
#include "file.h"

#include <unordered_map>
#include <pthread.h>
#include <iostream>

extern pthread_mutex_t buffer_manager_latch;

struct buffer_t {
    char frame[PAGE_SIZE];
    int64_t table_id;
    pagenum_t pagenum;
    bool is_dirty;

    // * is_pinned should be replaced by page latch
    bool is_pinned;
    pthread_mutex_t page_latch;

    struct buffer_t* next;
    struct buffer_t* prev;

    buffer_t();
};

class BufferManager {
    /* field */
    buffer_t* buf_head;
    buffer_t* buf_tail;
    std::vector<buffer_t*> buf_pool;
    std::unordered_map<int64_t, buffer_t*> hash_pointer;
    int max_count;
    int cur_count;

    private:
        /* set buffer initial value */
        void set_buf(buffer_t* buf, int64_t table_id, pagenum_t pagenum);
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
        void init_buf(int max_count);
        /* unpin buffer */
        void unpin_buffer(int64_t table_id, pagenum_t pagenum);
        /* open table */
        int64_t buffer_open_table_file(const char* pathname);
        /* allocate page */
        pagenum_t buffer_alloc_page(int64_t table_id);
        /* read page through buffer */
        buffer_t* buffer_read_page(int64_t table_id, pagenum_t pagenum);
        /* write page on buffer block */
        void buffer_write_page(int64_t table_id, pagenum_t pagenum); 
        /* free page */
        void buffer_free_page(int64_t table_id, pagenum_t pagenum);
        /* close table */
        void buffer_close_table_file();
        /* flush all buffer blocks and de-allocate buffer blocks */
        void destroy_all();
        /* Destructor */
        ~BufferManager();
};

extern BufferManager buffer_manager;

#endif