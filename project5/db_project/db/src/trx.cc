#include "trx.h"

#include <iostream>

pthread_mutex_t trx_manager_latch = PTHREAD_MUTEX_INITIALIZER;
extern pthread_mutex_t lock_table_latch;

int64_t global_trx_id;
TrxManager trx_manager;

void TrxManager::remove_trx_node(int trx_id) {
    trx_adj.erase(trx_id);
    for(auto& node : trx_adj) {
        auto& adj = node.second;
        adj.erase(trx_id);
    }
}
bool TrxManager::dfs(int u, std::map<int, bool>& visited, std::map<int, bool>& dfs_stk) {
    visited[u] = true;
    dfs_stk[u] = true;

    for(const int& v : trx_adj[u]) {
        if(!visited[v] && dfs(v, visited, dfs_stk)) return true;
        else if(dfs_stk[v]) return true;
    }

    dfs_stk[u] = false;
    return false;
}
bool TrxManager::is_deadlock(int trx_id) {
    std::map<int, bool> visited;
    std::map<int, bool> dfs_stk;

    return dfs(trx_id, visited, dfs_stk);
}
void TrxManager::undo_actions(int trx_id) {
    auto& log_stack = trx_log_table[trx_id];

    while(!log_stack.empty()) {
        auto& log = log_stack.top();

        buffer_t* page = buffer_manager.buffer_read_page(log.table_id, log.page_id);
        buffer_manager.buffer_write_page(log.table_id, log.page_id);
        slotnum_t offset = page_io::leaf::get_offset((page_t*)page->frame, log.slot_num);
        page_io::leaf::set_record((page_t*)page->frame, log.slot_num, log.old_value.c_str(), log.old_val_size);
        buffer_manager.unpin_buffer(log.table_id, log.page_id);

        log_stack.pop();
    }

    trx_log_table.erase(trx_id);
}
void TrxManager::start_trx(int trx_id) {
    trx_table.insert({trx_id, nullptr});
}
void TrxManager::remove_trx(int trx_id) {
    pthread_mutex_lock(&lock_table_latch);
    lock_t* cur_lock_obj = trx_table[trx_id];

    while(cur_lock_obj != nullptr) {
        lock_t* next_lock_obj = cur_lock_obj->next_trx_lock_obj;
        lock_release(cur_lock_obj);
        cur_lock_obj = next_lock_obj;
    }

    pthread_mutex_unlock(&lock_table_latch);

    remove_trx_node(trx_id);
    trx_log_table.erase(trx_id);

    trx_table.erase(trx_id);
}
void TrxManager::abort_trx(int trx_id) {
    undo_actions(trx_id);
    remove_trx(trx_id);
}
void TrxManager::add_action(int trx_id, lock_t* lock_obj) {
    pthread_mutex_lock(&lock_table_latch);

    lock_obj->next_trx_lock_obj = trx_table[trx_id];
    trx_table[trx_id] = lock_obj;

    pthread_mutex_unlock(&lock_table_latch);
}
void TrxManager::update_graph(lock_t* lock_obj) {
    pthread_mutex_lock(&lock_table_latch);

    // find preceding lock
    lock_t* cur_lock_obj = lock_obj->prev;
    while(cur_lock_obj != lock_obj->sentinel->head) {
        // X / S lock <- X lock
        if(lock_obj->lock_mode == EXCLUSIVE_LOCK) {
            if(cur_lock_obj->record_id == lock_obj->record_id
            && cur_lock_obj->owner_trx_id != lock_obj->owner_trx_id) {

                if(cur_lock_obj->lock_mode == SHARED_LOCK)
                    trx_adj[lock_obj->owner_trx_id].insert(cur_lock_obj->owner_trx_id);
                else if(cur_lock_obj->lock_mode == EXCLUSIVE_LOCK) {
                    trx_adj[lock_obj->owner_trx_id].insert(cur_lock_obj->owner_trx_id);
                    break;
                }

            }
        }

        // X lock <- S lock
        else {
            if(cur_lock_obj->record_id == lock_obj->record_id
            && cur_lock_obj->owner_trx_id != lock_obj->owner_trx_id
            && cur_lock_obj->lock_mode == EXCLUSIVE_LOCK) {
                trx_adj[lock_obj->owner_trx_id].insert(cur_lock_obj->owner_trx_id);
                break;
            }
        }

        cur_lock_obj = cur_lock_obj->prev;
    }

    pthread_mutex_unlock(&lock_table_latch);
}
void TrxManager::add_log_to_trx(int64_t table_id, pagenum_t page_id, slotnum_t slot_num, int trx_id) {
    char* old_value = nullptr;
    int old_val_size;

    buffer_t* page = buffer_manager.buffer_read_page(table_id, page_id);
    buffer_manager.buffer_write_page(table_id, page_id);
    slotnum_t offset = page_io::leaf::get_offset((page_t*)page->frame, slot_num);
    old_val_size = page_io::leaf::get_record_size((page_t*)page->frame, slot_num);
    old_value = new char[old_val_size];
    page_io::leaf::get_record((page_t*)page->frame, offset, old_value, old_val_size);
    buffer_manager.unpin_buffer(table_id, page_id);

    trx_log_table[trx_id].push({std::string(old_value, old_val_size), old_val_size, table_id, page_id, slot_num});
    delete[] old_value;
}

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

int trx_get_lock(int64_t table_id, pagenum_t page_id, slotnum_t slot_num, int trx_id, int lock_mode) {
    pthread_mutex_lock(&trx_manager_latch);

    lock_t* lock_obj = lock_acquire(table_id, page_id, slot_num, trx_id, lock_mode);
    trx_manager.add_action(trx_id, lock_obj);
    trx_manager.update_graph(lock_obj);
    
    if(trx_manager.is_deadlock(trx_id)) {
        trx_manager.abort_trx(trx_id);
        pthread_mutex_unlock(&trx_manager_latch);
        return -1;
    }
    
    while(is_conflict(lock_obj)) {
        pthread_cond_wait(&lock_obj->cond, &trx_manager_latch);
    }

    if(lock_mode == EXCLUSIVE_LOCK) 
        trx_manager.add_log_to_trx(table_id, page_id, slot_num, trx_id);

    pthread_mutex_unlock(&trx_manager_latch);

    return 0;
}