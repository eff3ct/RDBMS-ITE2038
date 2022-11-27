#include "lock_table.h"
#include "trx.h"
#include <iostream>

typedef struct lock_t lock_t;
typedef struct lock_table_entry_t lock_table_entry_t;

std::unordered_map<int64_t, lock_table_entry_t*> lock_table;

pthread_mutex_t lock_table_latch;

void print_all_locks(lock_table_entry_t* entry) {
    lock_t* lock = entry->head->next;

    std::cout << "lock list print start" << std::endl;
    while(lock != entry->tail) {
        std::cout << lock->record_id << "r, " << lock->owner_trx_id << "t, " << (lock->lock_mode == 0 ? "S" : "X") << std::endl;
        lock = lock->next;
    }
    std::cout << "lock list print end" << std::endl;
}

void unlink_and_awake_threads(lock_t* lock_obj) {
    lock_t* cur_lock_obj = lock_obj->next;
    
    lock_obj->prev->next = lock_obj->next;
    lock_obj->next->prev = lock_obj->prev;

    while(cur_lock_obj != lock_obj->sentinel->tail) {
        pthread_cond_signal(&cur_lock_obj->cond);
        cur_lock_obj = cur_lock_obj->next;
    }

    // int cnt = 0;

    // while(cur_lock_obj != lock_obj->sentinel->tail) {
    //     if(lock_obj->lock_mode == EXCLUSIVE_LOCK
    //     && lock_obj->owner_trx_id != cur_lock_obj->owner_trx_id) {
    //         if(cur_lock_obj->record_id == lock_obj->record_id) {
    //             if(cur_lock_obj->lock_mode == SHARED_LOCK) pthread_cond_signal(&cur_lock_obj->cond);
    //             else {
    //                 pthread_cond_signal(&cur_lock_obj->cond);
    //                 break;
    //             }
    //         }
    //     }
    //     else if(lock_obj->lock_mode == SHARED_LOCK
    //     && lock_obj->owner_trx_id != cur_lock_obj->owner_trx_id) {
    //         if(cur_lock_obj->record_id == lock_obj->record_id
    //         && cur_lock_obj->lock_mode == EXCLUSIVE_LOCK) {
    //             pthread_cond_signal(&cur_lock_obj->cond);
    //             break;
    //         }
    //     }

    //     // if(cur_lock_obj->record_id != lock_obj->record_id) {
    //     //     cur_lock_obj = cur_lock_obj->next;
    //     //     continue;
    //     // }

    //     // if(lock_obj->lock_mode == EXCLUSIVE_LOCK) {
    //     //     if(cur_lock_obj->lock_mode == EXCLUSIVE_LOCK
    //     //     && cnt == 0) {
    //     //         pthread_cond_signal(&cur_lock_obj->cond);
    //     //         break;
    //     //     }
    //     //     else if(cur_lock_obj->lock_mode == EXCLUSIVE_LOCK
    //     //     && cnt) {
    //     //         break;
    //     //     }
    //     //     else if(cur_lock_obj->lock_mode == SHARED_LOCK) {
    //     //         pthread_cond_signal(&cur_lock_obj->cond);
    //     //         cnt++;
    //     //     }
    //     // }
    //     // else if(lock_obj->lock_mode == SHARED_LOCK) {
    //     //     if(cur_lock_obj->lock_mode == EXCLUSIVE_LOCK) {
    //     //         pthread_cond_signal(&cur_lock_obj->cond);
    //     //         break;
    //     //     }
    //     // }
            
    //     // TODO : X락 해제 -> 모든 S락을 풀어주기 or 하나의 X락을 풀어주기 | S락 해제 -> 하나의 X락만 풀어주기

    //     cur_lock_obj = cur_lock_obj->next;
    // }

    delete lock_obj;
}
 
bool is_conflict(lock_t* lock_obj) {
    lock_t* cur_lock_obj = lock_obj->prev;

    while(cur_lock_obj != lock_obj->sentinel->head) {
        if(lock_obj->lock_mode == EXCLUSIVE_LOCK
        && lock_obj->owner_trx_id != cur_lock_obj->owner_trx_id) {
            if(cur_lock_obj->record_id == lock_obj->record_id) {
                return true;
            }
        }
        else if(lock_obj->lock_mode == SHARED_LOCK
        && lock_obj->owner_trx_id != cur_lock_obj->owner_trx_id) {
            if(cur_lock_obj->record_id == lock_obj->record_id
            && cur_lock_obj->lock_mode == EXCLUSIVE_LOCK) return true;
        }

        cur_lock_obj = cur_lock_obj->prev;
    }

    return false;
}

int init_lock_table() {
    lock_table = {};
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
        if(lock_table[combined_key]->tail->prev != lock_table[combined_key]->head) {            // // * CASE : there is a S lock in lock table and current lock mode is X.
            // Project 4 implementation below.
            /* there is already lock object */
            lock_t* lock_obj = new lock_t(key, trx_id, lock_mode);
            lock_obj->sentinel = lock_table[combined_key];

            /* insert into tail */
            lock_obj->prev = lock_table[combined_key]->tail->prev;
            lock_obj->next = lock_table[combined_key]->tail;
            lock_table[combined_key]->tail->prev->next = lock_obj;
            lock_table[combined_key]->tail->prev = lock_obj;

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
