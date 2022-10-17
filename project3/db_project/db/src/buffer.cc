#include "buffer.h"

buffer_t::buffer_t() {
    table_id = -1;
    pagenum = -1;
    is_dirty = false;
    is_pinned = 0;
    next = nullptr;
    prev = nullptr;
}

buffer_t::buffer_t(int64_t table_id, pagenum_t pagenum) {
    this->table_id = table_id;
    this->pagenum = pagenum;
    is_dirty = false;
    is_pinned = 0;
    next = nullptr;
    prev = nullptr;
}

/* private */
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

    buf->is_pinned = 0;
}

void BufferManager::flush_buffer(int64_t table_id, pagenum_t pagenum) {
    if(!is_buffer_exist(table_id, pagenum)) return;
    
    buffer_t* cur_buf = find_buffer(table_id, pagenum);
    if(cur_buf->is_dirty) {
        file_write_page(table_id, pagenum, (page_t*)cur_buf->frame);
        cur_buf->is_dirty = false;
    }

    cur_buf->is_pinned = 0;
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
    delete buf_head;
    delete buf_tail;
}

void BufferManager::set_max_count(int max_count) { this->max_count = max_count; }

void BufferManager::unpin_buffer(int64_t table_id, pagenum_t pagenum) {
    if(!is_buffer_exist(table_id, pagenum)) return;
    buffer_t* cur_buf = find_buffer(table_id, pagenum);
    cur_buf->is_pinned--;
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
    hash_pointer.clear();
    buf_head->next = buf_tail;
    buf_tail->prev = buf_head;
}

void BufferManager::buffer_read_page(int64_t table_id, pagenum_t pagenum, page_t* dest) {
    /* cache miss */
    if(!is_buffer_exist(table_id, pagenum)) {
        /* when buffer is full */
        if(cur_count == max_count) {
            buffer_t* victim = find_victim();
            if(victim->is_dirty) flush_buffer(victim);
            int64_t key = convert_pair_to_key(victim->table_id, victim->pagenum);
            hash_pointer.erase(key);
            
            // delete victim
            victim->prev->next = victim->next;
            victim->next->prev = victim->prev;
            delete victim;

            // insert new buffer
            buffer_t* new_buf = new buffer_t(table_id, pagenum);
            file_read_page(table_id, pagenum, (page_t*)new_buf->frame);
            insert_into_head(new_buf);
            new_buf->is_pinned++;

            // insert into hash
            int64_t new_key = convert_pair_to_key(table_id, pagenum);
            hash_pointer.insert({new_key, new_buf});
        }
        /* buffer is not full */
        else {
            buffer_t* new_buf = new buffer_t(table_id, pagenum);
            file_read_page(table_id, pagenum, (page_t*)new_buf->frame);
            insert_into_head(new_buf);
            new_buf->is_pinned++;

            // insert into hash
            int64_t new_key = convert_pair_to_key(table_id, pagenum);
            hash_pointer.insert({new_key, new_buf});
            cur_count++;
        }
    }
    /* cache hit */
    else {
        buffer_t* cur_buf = find_buffer(table_id, pagenum);
        memcpy(dest, cur_buf->frame, PAGE_SIZE);
        cur_buf->is_pinned++;

        // move to front
        move_to_head(cur_buf);
    }
}

void BufferManager::buffer_write_page(int64_t table_id, pagenum_t pagenum, const page_t* src) {
    /* cache miss */
    if(!is_buffer_exist(table_id, pagenum)) {
        /* when buffer is full */
        if(cur_count == max_count) {
            buffer_t* victim = find_victim();
            if(victim->is_dirty) flush_buffer(victim);
            int64_t key = convert_pair_to_key(victim->table_id, victim->pagenum);
            hash_pointer.erase(key);
            
            // delete victim
            victim->prev->next = victim->next;
            victim->next->prev = victim->prev;
            delete victim;

            // insert new buffer
            buffer_t* new_buf = new buffer_t(table_id, pagenum);
            memcpy(new_buf->frame, src, PAGE_SIZE);
            new_buf->is_dirty = true;
            new_buf->is_pinned++;
            insert_into_head(new_buf);

            // insert into hash
            int64_t new_key = convert_pair_to_key(table_id, pagenum);
            hash_pointer.insert({new_key, new_buf});
        }
        /* buffer is not full */
        else {
            buffer_t* new_buf = new buffer_t(table_id, pagenum);
            memcpy(new_buf->frame, src, PAGE_SIZE);
            new_buf->is_dirty = true;
            new_buf->is_pinned++;
            insert_into_head(new_buf);

            // insert into hash
            int64_t new_key = convert_pair_to_key(table_id, pagenum);
            hash_pointer.insert({new_key, new_buf});
            cur_count++;
        }
    }
    /* cache hit */
    else {
        buffer_t* cur_buf = find_buffer(table_id, pagenum);
        memcpy(cur_buf->frame, src, PAGE_SIZE);
        cur_buf->is_dirty = true;
        cur_buf->is_pinned++;

        // move to front
        move_to_head(cur_buf);
    }
}

void BufferManager::buffer_free_page(int64_t table_id, pagenum_t pagenum) {
    // TODO
    // 1. implement here
}

pagenum_t BufferManager::buffer_alloc_page(int64_t table_id) {
    // TODO
    // 1. implement here
}