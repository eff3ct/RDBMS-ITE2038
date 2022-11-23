#include "trx.h"

#include <iostream>

pthread_mutex_t trx_manager_latch = PTHREAD_MUTEX_INITIALIZER;
int global_trx_id;
TrxManager trx_manager;

bool TrxManager::is_lock_exist(int trx_id, lock_t* lock_obj) {
    lock_t* cur_lock_obj = trx_table[trx_id];
    while(cur_lock_obj != nullptr) {
        if(cur_lock_obj == lock_obj) return true;
        cur_lock_obj = cur_lock_obj->next;
    }
    return false;
}

void TrxManager::start_trx(int trx_id) {
    trx_table.insert({trx_id, nullptr});
}
void TrxManager::remove_trx(int trx_id) {
    lock_t* cur_lock_obj = trx_table[trx_id];

    while(cur_lock_obj != nullptr) {
        lock_t* next_lock_obj = cur_lock_obj->next_trx_lock_obj;
        lock_release(cur_lock_obj);
        cur_lock_obj = next_lock_obj;
    }

    trx_table.erase(trx_id);
}
void TrxManager::add_action(int trx_id, lock_t* lock_obj) {
    // check lock_obj is already in trx_table[trx_id]
    if(is_lock_exist(trx_id, lock_obj)) return;
    
    if(trx_table[trx_id] == nullptr) {
        trx_table[trx_id] = lock_obj;
        lock_obj->next_trx_lock_obj = nullptr;
    }
    else {
        lock_t* cur_ptr = trx_table[trx_id];
        while(cur_ptr->next_trx_lock_obj != nullptr) 
            cur_ptr = cur_ptr->next_trx_lock_obj;
        cur_ptr->next_trx_lock_obj = lock_obj;
        lock_obj->next_trx_lock_obj = nullptr;
    }
}

// TODO : Transaction abort control (deadlock)
int trx_begin() {
    pthread_mutex_lock(&trx_manager_latch);

    int trx_id = ++global_trx_id;
    trx_manager.start_trx(trx_id);

    pthread_mutex_unlock(&trx_manager_latch);
    return trx_id;
}

int trx_commit(int trx_id) {
    pthread_mutex_lock(&trx_manager_latch);

    trx_manager.remove_trx(trx_id);

    pthread_mutex_unlock(&trx_manager_latch);

    return trx_id;
}