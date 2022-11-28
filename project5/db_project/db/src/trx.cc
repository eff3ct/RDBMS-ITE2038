#include "trx.h"

#include <iostream>

pthread_mutex_t trx_manager_latch = PTHREAD_MUTEX_INITIALIZER;
int64_t global_trx_id;
TrxManager trx_manager;

bool TrxManager::is_deadlock(int trx_id) {
    std::vector<bool> visited(trx_adj.size(), false);
    std::vector<bool> dfs_stk(trx_adj.size(), false);

    std::function<bool(int)> dfs = [&](int u) {
        visited[u] = true;
        dfs_stk[u] = true;

        for(const int& v : trx_adj[u]) {
            if(!visited[v] && dfs(v)) return true;
            else if(dfs_stk[v]) return true;
        }

        dfs_stk[u] = false;
        return false;
    };

    return dfs(trx_id);
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
    lock_obj->next_trx_lock_obj = trx_table[trx_id];
    trx_table[trx_id] = lock_obj;
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

void trx_get_lock(int64_t table_id, pagenum_t page_id, slotnum_t slot_num, int trx_id, int lock_mode) {
    pthread_mutex_lock(&trx_manager_latch);

    lock_t* lock_obj = lock_acquire(table_id, page_id, slot_num, trx_id, lock_mode);
    trx_manager.add_action(trx_id, lock_obj);

    while(is_conflict(lock_obj)) {
        pthread_cond_wait(&lock_obj->cond, &trx_manager_latch);
    }

    pthread_mutex_unlock(&trx_manager_latch);
}