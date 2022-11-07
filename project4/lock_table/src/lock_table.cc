#include "lock_table.h"

struct lock_t {
    lock_t* prev;
    lock_t* next;
    lock_table_entry_t* sentinel;
    pthread_cond_t cond;

    lock_t() {
        prev = nullptr;
        next = nullptr;
        sentinel = nullptr;
        cond = PTHREAD_COND_INITIALIZER;
    }
};

struct lock_table_entry_t {
    int64_t table_id;
    int64_t record_id;
    lock_t* head;
    lock_t* tail;

    lock_table_entry_t(int64_t table_id, int64_t record_id) {
        this->table_id = table_id;
        this->record_id = record_id;
        this->head = new lock_t();
        this->tail = new lock_t();
        this->head->next = this->tail;
        this->tail->prev = this->head;
    }
};

typedef struct lock_t lock_t;
typedef struct lock_table_entry_t lock_table_entry_t;

std::unordered_map<int64_t, lock_table_entry_t*> lock_table;

pthread_mutex_t lock_table_latch;

int init_lock_table() {
    lock_table_latch = PTHREAD_MUTEX_INITIALIZER;
    return 0;
}

lock_t* lock_acquire(int64_t table_id, int64_t key) {
    int64_t combined_key = (table_id << 32) | key;

    lock_t* ret_obj = NULL;
    pthread_mutex_lock(&lock_table_latch);
    if(lock_table.find(combined_key) == lock_table.end()) {
        lock_table.insert({combined_key, new lock_table_entry_t(table_id, key)});

        lock_t* lock_obj = new lock_t();
        lock_obj->sentinel = lock_table[combined_key];

        /* link node */
        lock_table[combined_key]->head->next = lock_obj;
        lock_obj->prev = lock_table[combined_key]->head;
        lock_obj->next = lock_table[combined_key]->tail;
        lock_table[combined_key]->tail->prev = lock_obj;

        ret_obj = lock_obj;
    }
    else {
        if(lock_table[combined_key]->tail->prev != lock_table[combined_key]->head) {
            /* there is already lock object */
            lock_t* lock_obj = new lock_t();
            lock_obj->sentinel = lock_table[combined_key];

            /* insert into tail */
            lock_obj->prev = lock_table[combined_key]->tail->prev;
            lock_obj->next = lock_table[combined_key]->tail;
            lock_table[combined_key]->tail->prev->next = lock_obj;
            lock_table[combined_key]->tail->prev = lock_obj;

            while(lock_table[combined_key]->head->next != lock_obj)
                pthread_cond_wait(&lock_obj->cond, &lock_table_latch);

            ret_obj = lock_obj;
        }
        else {
            /* there is no lock object */
            lock_t* lock_obj = new lock_t();
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
    lock_t* next_obj = lock_obj->next;

    /* unlink node */
    lock_obj->prev->next = lock_obj->next;
    lock_obj->next->prev = lock_obj->prev;
    if(next_obj != lock_obj->sentinel->tail) {
        /* there is lock object after this lock object */
        pthread_cond_signal(&next_obj->cond);
    }

    delete lock_obj;

    pthread_mutex_unlock(&lock_table_latch);

    return 0;
}
