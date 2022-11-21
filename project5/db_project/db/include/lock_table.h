#ifndef __LOCK_TABLE_H__
#define __LOCK_TABLE_H__

#include <stdint.h>
#include <pthread.h>
#include <unordered_map>

#define SHARED_LOCK 0
#define EXCLUSIVE_LOCK 1

typedef struct lock_table_entry_t lock_table_entry_t;
typedef struct lock_t lock_t;
typedef uint64_t pagenum_t;
typedef int16_t slotnum_t;

struct lock_t {
    lock_t* prev;
    lock_t* next;
    lock_table_entry_t* sentinel;
    pthread_cond_t cond;
    int lock_mode;
    pagenum_t record_id;
    lock_t* next_trx_lock_obj;
    int owner_trx_id;

    lock_t() {
        prev = nullptr;
        next = nullptr;
        sentinel = nullptr;
        cond = PTHREAD_COND_INITIALIZER;
    }

    lock_t(pagenum_t record_id, int trx_id, int lock_mode) {
        prev = nullptr;
        next = nullptr;
        sentinel = nullptr;
        cond = PTHREAD_COND_INITIALIZER;
        this->lock_mode = lock_mode;
        this->record_id = record_id;
        this->owner_trx_id = trx_id;
        this->next_trx_lock_obj = nullptr;
    }
};

struct lock_table_entry_t {
    int64_t table_id;
    int64_t page_id;
    lock_t* head;
    lock_t* tail;

    lock_table_entry_t(int64_t table_id, int64_t page_id) {
        this->table_id = table_id;
        this->page_id = page_id;
        this->head = new lock_t();
        this->tail = new lock_t();
        this->head->next = this->tail;
        this->tail->prev = this->head;
    }
};

void unlink_and_awake_threads(lock_t* lock_obj);
bool is_conflict(lock_t* lock_obj, int lock_mode);

/* APIs for lock table */
int init_lock_table();
lock_t *lock_acquire(int64_t table_id, pagenum_t page_id, int64_t key, int trx_id, int lock_mode);
int lock_release(lock_t* lock_obj);

#endif /* __LOCK_TABLE_H__ */
