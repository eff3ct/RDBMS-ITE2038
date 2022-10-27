#include "buffer.h"

BufferManager buffer_manager;

int total_cnt;
int cache_hit;

buffer_t::buffer_t() {
    table_id = -1;
    pagenum = -1;
    is_dirty = false;
    is_pinned = false;
    next = nullptr;
    prev = nullptr;
}

/* private */
void BufferManager::set_buf(buffer_t* buf, int64_t table_id, pagenum_t pagenum) {
    buf->table_id = table_id;
    buf->pagenum = pagenum;
}

void BufferManager::move_to_head(buffer_t* buf) {
    buf->prev->next = buf->next;
    buf->next->prev = buf->prev;
    buf->next = buf_head->next;
    buf->prev = buf_head;
    buf_head->next->prev = buf;
    buf_head->next = buf;
}

void BufferManager::insert_into_head(buffer_t* buf) {
    buf->next = buf_head->next;
    buf->prev = buf_head;
    buf_head->next->prev = buf;
    buf_head->next = buf;
}

int64_t BufferManager::convert_pair_to_key(int64_t table_id, pagenum_t pagenum) { return (table_id << 32LL) | pagenum; }

void BufferManager::flush_buffer(buffer_t* buf) {
    if(buf->is_dirty) {
        file_write_page(buf->table_id, buf->pagenum, (page_t*)buf->frame);
        buf->is_dirty = false;
    }

    buf->is_pinned = false;
}

void BufferManager::flush_buffer(int64_t table_id, pagenum_t pagenum) {
    if(!is_buffer_exist(table_id, pagenum)) return;
    
    buffer_t* cur_buf = find_buffer(table_id, pagenum);
    if(cur_buf->is_dirty) {
        file_write_page(table_id, pagenum, (page_t*)cur_buf->frame);
        cur_buf->is_dirty = false;
    }

    cur_buf->is_pinned = false;
}

bool BufferManager::is_buffer_exist(int64_t table_id, pagenum_t pagenum) {
    int64_t key = convert_pair_to_key(table_id, pagenum);
    return hash_pointer.find(key) != hash_pointer.end();
}

buffer_t* BufferManager::find_buffer(int64_t table_id, pagenum_t pagenum) {
    int64_t key = convert_pair_to_key(table_id, pagenum);
    return hash_pointer[key];
}

buffer_t* BufferManager::find_victim() {
    buffer_t* cur_buf = buf_tail->prev;
    while(cur_buf != buf_head) {
        if(!cur_buf->is_pinned) return cur_buf;
        cur_buf = cur_buf->prev;
    }

    std::cerr << "ERROR : Eviction failed. No victim buffer found. (All pinned)" << std::endl;
    exit(EXIT_FAILURE);

    return nullptr;
}

/* public */
BufferManager::BufferManager() {
    buf_head = new buffer_t();
    buf_tail = new buffer_t();

    buf_head->next = buf_tail;
    buf_tail->prev = buf_head;

    buf_head->prev = buf_tail->next = nullptr;
    max_count = cur_count = 0;
}

BufferManager::~BufferManager() {
    std::cout << "total cnt : " << total_cnt << std::endl;
    std::cout << "cache hit : " << cache_hit << std::endl;
    delete buf_head;
    delete buf_tail;
}

void BufferManager::init_buf(int max_count) { 
    this->cur_count = 0;
    this->max_count = max_count;
    buf_pool.resize(max_count);
    for(int i = 0; i < max_count; ++i) 
        buf_pool[i] = new buffer_t();
}

void BufferManager::unpin_buffer(int64_t table_id, pagenum_t pagenum) {
    if(!is_buffer_exist(table_id, pagenum)) return;
    buffer_t* cur_buf = find_buffer(table_id, pagenum);
    cur_buf->is_pinned = false;
}

int64_t BufferManager::buffer_open_table_file(const char* pathname) {
    int64_t table_id = file_open_table_file(pathname);
    return table_id;
}

void BufferManager::destroy_all() {
    buffer_t* cur_buf = buf_head->next;
    while(cur_buf != buf_tail) {
        if(cur_buf->is_dirty) {
            file_write_page(cur_buf->table_id, cur_buf->pagenum, (page_t*)cur_buf->frame);
            cur_buf->is_dirty = false;
        }
        buffer_t* tmp = cur_buf;
        cur_buf = cur_buf->next;
        delete tmp;
    }
    cur_count = 0;
    hash_pointer.clear();
    buf_head->next = buf_tail;
    buf_tail->prev = buf_head;
}

void BufferManager::buffer_close_table_file() {
    file_close_table_files();
}

buffer_t* BufferManager::buffer_read_page(int64_t table_id, pagenum_t pagenum) {
    total_cnt++;
    /* cache miss */
    if(!is_buffer_exist(table_id, pagenum)) {
        /* when buffer is full */
        if(cur_count == max_count) {
            buffer_t* victim = find_victim();
            if(victim->is_dirty) flush_buffer(victim);
            int64_t key = convert_pair_to_key(victim->table_id, victim->pagenum);
            hash_pointer.erase(key);

            // insert new buffer
            buffer_t* new_buf = victim;
            set_buf(new_buf, table_id, pagenum);
            file_read_page(table_id, pagenum, (page_t*)new_buf->frame);
            move_to_head(new_buf);
            new_buf->is_pinned = true;

            // insert into hash
            int64_t new_key = convert_pair_to_key(table_id, pagenum);
            hash_pointer.insert({new_key, new_buf});

            return new_buf;
        }
        /* buffer is not full */
        else {
            buffer_t* new_buf = buf_pool[cur_count++];
            set_buf(new_buf, table_id, pagenum);
            file_read_page(table_id, pagenum, (page_t*)new_buf->frame);
            insert_into_head(new_buf);
            new_buf->is_pinned = true;

            // insert into hash
            int64_t new_key = convert_pair_to_key(table_id, pagenum);
            hash_pointer.insert({new_key, new_buf});

            return new_buf;
        }
    }
    /* cache hit */
    else {
        cache_hit++;
        buffer_t* cur_buf = find_buffer(table_id, pagenum);
        cur_buf->is_pinned = true;

        // move to front
        move_to_head(cur_buf);
        return cur_buf;
    }

    return nullptr;
}

void BufferManager::buffer_write_page(int64_t table_id, pagenum_t pagenum) {
    if(!is_buffer_exist(table_id, pagenum)) return;
    buffer_t* cur_buf = find_buffer(table_id, pagenum);
    cur_buf->is_dirty = true;
}

void BufferManager::buffer_free_page(int64_t table_id, pagenum_t pagenum) {
    if(is_buffer_exist(table_id, pagenum)) {
        buffer_t* cur_buf = find_buffer(table_id, pagenum);
        cur_buf->is_dirty = false;
        int64_t key = convert_pair_to_key(table_id, pagenum);
        hash_pointer.erase(key);
        cur_buf->is_pinned = false;
    }
   
    if(!is_buffer_exist(table_id, 0)) {
        file_free_page(table_id, pagenum);
        return;
    }

    buffer_t* header_buf = find_buffer(table_id, 0);
    buffer_write_page(table_id, 0);

    page_t free_page;
    pagenum_t next_free_page_num = page_io::get_next_free_page((page_t*)header_buf->frame, 0);
    page_io::set_free_page_next(&free_page, next_free_page_num);
    file_write_page(table_id, pagenum, &free_page);

    page_io::header::set_next_free_page((page_t*)header_buf->frame, pagenum);
    header_buf->is_dirty = true;
}

pagenum_t BufferManager::buffer_alloc_page(int64_t table_id) {
    if(!is_buffer_exist(table_id, 0)) {
        pagenum_t new_pagenum = file_alloc_page(table_id);
        return new_pagenum;
    }
    
    buffer_t* header_buf = find_buffer(table_id, 0);
    buffer_write_page(table_id, 0);

    pagenum_t next_free_page_num = page_io::get_next_free_page((page_t*)header_buf->frame, 0);
    if(next_free_page_num == 0) {
        pagenum_t page_cnt = page_io::header::get_page_count((page_t*)header_buf->frame);
        pagenum_t curr_cnt = page_cnt * 2;

        page_io::set_free_page_next((page_t*)header_buf->frame, page_cnt);

        for(pagenum_t i = page_cnt; i < curr_cnt; i++) {
            page_t free_page;
            pagenum_t next_free_page_num = (i + 1) % curr_cnt;
            page_io::set_free_page_next(&free_page, next_free_page_num);
            file_write_page(table_id, i, &free_page);
        }

        pagenum_t root_page_num = page_io::header::get_root_page((page_t*)header_buf->frame);
        page_io::header::set_header_page((page_t*)header_buf->frame, page_cnt, curr_cnt, root_page_num);
    }

    page_t free_page;
    next_free_page_num = page_io::get_next_free_page((page_t*)header_buf->frame, 0);
    file_read_page(table_id, next_free_page_num, &free_page);

    pagenum_t header_next_free_page_num = page_io::get_next_free_page(&free_page, next_free_page_num);
    page_io::header::set_next_free_page((page_t*)header_buf->frame, header_next_free_page_num);
    header_buf->is_dirty = true;

    return next_free_page_num;
}