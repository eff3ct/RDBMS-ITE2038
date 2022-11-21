#include "lock_table.h"

typedef struct lock_t lock_t;
typedef struct lock_table_entry_t lock_table_entry_t;

std::unordered_map<int64_t, lock_table_entry_t*> lock_table;

pthread_mutex_t lock_table_latch;

void unlink_and_awake_threads(lock_t* lock_obj) {
    lock_t* cur_lock_obj = lock_obj->next;
    
    lock_obj->prev->next = lock_obj->next;
    lock_obj->next->prev = lock_obj->prev;

    while(cur_lock_obj != lock_obj->sentinel->tail) {
        if(lock_obj->lock_mode == EXCLUSIVE_LOCK) {
            if(cur_lock_obj->record_id == lock_obj->record_id) {
                if(cur_lock_obj->lock_mode == SHARED_LOCK) pthread_cond_signal(&cur_lock_obj->cond);
                else {
                    pthread_cond_signal(&cur_lock_obj->cond);
                    break;
                }
            }
        }
        else {
            if(cur_lock_obj->record_id == lock_obj->record_id
            && cur_lock_obj->lock_mode == EXCLUSIVE_LOCK) {
                pthread_cond_signal(&cur_lock_obj->cond);
                break;
            }
        }
    }

    delete lock_obj;
}
 
bool is_conflict(lock_t* lock_obj) {
    lock_t* cur_lock_obj = lock_obj;

    do {
        cur_lock_obj = cur_lock_obj->prev;
        if(cur_lock_obj->record_id == lock_obj->record_id
        && cur_lock_obj->lock_mode == EXCLUSIVE_LOCK)
            return true;
    }
    while(cur_lock_obj != lock_obj->sentinel->head);

    return false;
}

int init_lock_table() {
    lock_table_latch = PTHREAD_MUTEX_INITIALIZER;
    return 0;
}

lock_t* lock_acquire(int64_t table_id, pagenum_t page_id, int64_t key, int trx_id, int lock_mode) {
    int64_t combined_key = (table_id << 32) | page_id;

    lock_t* ret_obj = NULL;
    pthread_mutex_lock(&lock_table_latch);

    // * CASE : there is a NO combined_key entry in lock table.
    if(lock_table.find(combined_key) == lock_table.end()) {
        lock_table.insert({combined_key, new lock_table_entry_t(table_id, page_id)});

        lock_t* lock_obj = new lock_t(key, trx_id, lock_mode);
        lock_obj->sentinel = lock_table[combined_key];

        /* link node */
        lock_table[combined_key]->head->next = lock_obj;
        lock_obj->prev = lock_table[combined_key]->head;
        lock_obj->next = lock_table[combined_key]->tail;
        lock_table[combined_key]->tail->prev = lock_obj;

        ret_obj = lock_obj;
    }

    // * CASE : there is a combined_key entry.
    else {
        if(lock_table[combined_key]->tail->prev != lock_table[combined_key]->head) {
            /* there is already lock object */
            lock_t* lock_obj = new lock_t(key, trx_id, lock_mode);
            lock_obj->sentinel = lock_table[combined_key];

            /* insert into tail */
            lock_obj->prev = lock_table[combined_key]->tail->prev;
            lock_obj->next = lock_table[combined_key]->tail;
            lock_table[combined_key]->tail->prev->next = lock_obj;
            lock_table[combined_key]->tail->prev = lock_obj;

            while(is_conflict(lock_obj)) 
                pthread_cond_wait(&lock_obj->cond, &lock_table_latch);

            ret_obj = lock_obj;
        }
        else {
            /* there is no lock object */
            lock_t* lock_obj = new lock_t(key, trx_id, lock_mode);
            lock_obj->sentinel = lock_table[combined_key];

            /* link node */
            lock_table[combined_key]->head->next = lock_obj;
            lock_obj->prev = lock_table[combined_key]->head;
            lock_obj->next = lock_table[combined_key]->tail;
            lock_table[combined_key]->tail->prev = lock_obj;

            ret_obj = lock_obj;
        }
    }
    pthread_mutex_unlock(&lock_table_latch);

    return ret_obj;
};

int lock_release(lock_t* lock_obj) {
    pthread_mutex_lock(&lock_table_latch);
    
    unlink_and_awake_threads(lock_obj);

    pthread_mutex_unlock(&lock_table_latch);

    return 0;
}
