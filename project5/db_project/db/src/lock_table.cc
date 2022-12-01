#include "lock_table.h"
#include "trx.h"
#include <iostream>

typedef struct lock_t lock_t;
typedef struct lock_table_entry_t lock_table_entry_t;

pthread_cond_t gcond = PTHREAD_COND_INITIALIZER;

std::unordered_map<int64_t, lock_table_entry_t*> lock_table;

pthread_mutex_t lock_table_latch;

void wake_all() {
    pthread_mutex_lock(&lock_table_latch);

    // for(auto& entry : lock_table) {
    //     auto& lock_table_entry = entry.second;
        
    //     lock_t* cur_lock = lock_table_entry->head->next;
    //     while(cur_lock != lock_table_entry->tail) {
    //         pthread_cond_signal(&cur_lock->cond);
    //         cur_lock = cur_lock->next;
    //     }
    // }
    pthread_cond_broadcast(&gcond);

    pthread_mutex_unlock(&lock_table_latch);
}
void print_all_locks(lock_table_entry_t* entry) {
    lock_t* lock = entry->head->next;

    std::cout << "lock list print start" << std::endl;
    while(lock != entry->tail) {
        std::cout << lock->record_id << "r, " << lock->owner_trx_id << "t, " << (lock->lock_mode == 0 ? "S" : "X") << std::endl;
        lock = lock->next;
    }
    std::cout << "lock list print end" << std::endl;
}

void unlink_and_wake_threads(lock_t* lock_obj) {
    lock_t* cur_lock_obj = lock_obj->sentinel->head->next;
    if(cur_lock_obj == lock_obj) cur_lock_obj = cur_lock_obj->next;
    lock_t* tail = lock_obj->sentinel->tail;
    pagenum_t record_id = lock_obj->record_id;
    int owner_trx_id = lock_obj->owner_trx_id;

    lock_obj->prev->next = lock_obj->next;
    lock_obj->next->prev = lock_obj->prev;

    delete lock_obj;

    while(cur_lock_obj != tail) {
        if(cur_lock_obj->record_id != record_id
        || cur_lock_obj->owner_trx_id == owner_trx_id) {
            cur_lock_obj = cur_lock_obj->next;
            continue;
        }

        pthread_cond_signal(&cur_lock_obj->cond);

        cur_lock_obj = cur_lock_obj->next;
    }
}
 
bool is_conflict(lock_t* lock_obj) {
    pthread_mutex_lock(&lock_table_latch);
    lock_t* cur_lock_obj = lock_obj->prev;

    while(cur_lock_obj != lock_obj->sentinel->head) {
        if(lock_obj->lock_mode == EXCLUSIVE_LOCK
        && lock_obj->owner_trx_id != cur_lock_obj->owner_trx_id) {
            if(cur_lock_obj->record_id == lock_obj->record_id) {
                pthread_mutex_unlock(&lock_table_latch);
                return true;
            }
        }
        else if(lock_obj->lock_mode == SHARED_LOCK
        && lock_obj->owner_trx_id != cur_lock_obj->owner_trx_id) {
            if(cur_lock_obj->record_id == lock_obj->record_id
            && cur_lock_obj->lock_mode == EXCLUSIVE_LOCK) {
                pthread_mutex_unlock(&lock_table_latch);
                return true;
            }
        }

        cur_lock_obj = cur_lock_obj->prev;
    }
    pthread_mutex_unlock(&lock_table_latch);

    return false;
}

int init_lock_table() {
    lock_table = {};
    lock_table_latch = PTHREAD_MUTEX_INITIALIZER;
    return 0;
}

lock_t* lock_acquire(int64_t table_id, pagenum_t page_id, int64_t key, int trx_id, int lock_mode) {
    int64_t combined_key = (table_id << 32) | page_id;

    lock_t* ret_obj = nullptr;
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
            // check already lock exists in lock table.
            bool flag = false;
            int s_lock_cnt = 0, x_lock_cnt = 0;

            if(lock_mode == SHARED_LOCK) {
                lock_t* cur_lock_obj = lock_table[combined_key]->head->next;
                while(cur_lock_obj != lock_table[combined_key]->tail) {
                    if(cur_lock_obj->owner_trx_id == trx_id
                    && cur_lock_obj->record_id == key) {
                        flag = true;
                        ret_obj = nullptr;
                        pthread_mutex_unlock(&lock_table_latch);
                        return ret_obj;
                    }
                    cur_lock_obj = cur_lock_obj->next;
                }
            }
            else {
                lock_t* cur_lock_obj = lock_table[combined_key]->head->next;
                while(cur_lock_obj != lock_table[combined_key]->tail) {
                    if(cur_lock_obj->owner_trx_id == trx_id
                    && cur_lock_obj->record_id == key) {
                        if(cur_lock_obj->lock_mode == EXCLUSIVE_LOCK) {
                            flag = true;
                            ret_obj = nullptr;
                            pthread_mutex_unlock(&lock_table_latch);
                            return ret_obj;
                        }
                        else if(cur_lock_obj->lock_mode == SHARED_LOCK) {
                            flag = true;
                            ret_obj = cur_lock_obj;
                        }
                    }
                    else if(cur_lock_obj->owner_trx_id != trx_id
                    && cur_lock_obj->record_id == key) {
                        if(cur_lock_obj->lock_mode == SHARED_LOCK) s_lock_cnt++;
                        else x_lock_cnt++;
                    }
                    cur_lock_obj = cur_lock_obj->next;
                }
            }

            // S -> X conversion
            if(flag && (s_lock_cnt == 0 && x_lock_cnt == 0)) {
                ret_obj->lock_mode = EXCLUSIVE_LOCK;
                pthread_mutex_unlock(&lock_table_latch);
                return nullptr;
            }

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
    unlink_and_wake_threads(lock_obj);
    return 0;
}
