#include "trx.h"

pthread_mutex_t trx_manager_latch;
int global_trx_id;
TrxManager trx_manager;

/* Transaction Manager Definition */
TrxManager::TrxManager() {
    trx_manager_latch = PTHREAD_MUTEX_INITIALIZER;
    global_trx_id = 0;
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