#include "trx.h"

#include <iostream>

pthread_mutex_t trx_manager_latch = PTHREAD_MUTEX_INITIALIZER;
extern pthread_mutex_t lock_table_latch;
extern LogBufferManager log_buf_manager;

int64_t global_trx_id;
TrxManager trx_manager;

void TrxManager::init() {
    pthread_mutex_init(&trx_manager_latch, NULL);
    global_trx_id = 0;
    trx_table = {};
    trx_adj = {};
    trx_log_table = {};
}
void TrxManager::remove_trx_node(int trx_id) {
    trx_adj.erase(trx_id);
    for(auto& node : trx_adj) {
        auto& adj = node.second;
        adj.erase(trx_id);
    }
}
bool TrxManager::is_deadlock(int trx_id) {
    std::set<int> visited;
    visited.insert(trx_id);

    std::stack<std::pair<int, int>> dfs_stk;
    dfs_stk.push({ trx_id, trx_id });
    while(!dfs_stk.empty()) {
        int prev = dfs_stk.top().first;
        int curr = dfs_stk.top().second;

        dfs_stk.pop();

        if(prev != curr) {
            if(trx_id == curr) return true;
            if(visited.find(curr) != visited.end()) continue;
        } 

        visited.insert(curr);

        for(const int& next : trx_adj[curr]) {
            dfs_stk.push({ curr, next });
        }
    }

    return false;
}
void TrxManager::undo_actions(int trx_id) {
    auto& log_stack = trx_log_table[trx_id];

    while(!log_stack.empty()) {
        auto& log = log_stack.top();

        buffer_t* page = buffer_manager.buffer_read_page(log.table_id, log.page_id);
        buffer_manager.buffer_write_page(log.table_id, log.page_id);
        slotnum_t offset = page_io::leaf::get_offset((page_t*)page->frame, log.slot_num);

        char* buf = new char[log.old_val_size];
        page_io::leaf::get_record((page_t*)page->frame, offset, buf, log.old_val_size);
        std::string cur_value = std::string(buf, log.old_val_size);
        delete[] buf;

        update_log_t* real_log = new update_log_t(trx_id, log.table_id, log.page_id, offset, log.old_val_size, cur_value, log.old_value);
        log_buf_manager.add_log(real_log);
        
        page_io::leaf::set_record((page_t*)page->frame, offset, log.old_value.c_str(), log.old_val_size);
        buffer_manager.unpin_buffer(log.table_id, log.page_id);

        log_stack.pop();
    }

    trx_log_table.erase(trx_id);
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

    remove_trx_node(trx_id);
    trx_log_table.erase(trx_id);
    trx_table.erase(trx_id);
}
void TrxManager::abort_trx(int trx_id) {
    undo_actions(trx_id);
    remove_trx(trx_id);

    rollback_log_t* log = new rollback_log_t(trx_id);
    log_buf_manager.add_log(log);

    //print_adj();
    pthread_mutex_unlock(&trx_manager_latch);
    //wake_all();
}
void TrxManager::add_action(int trx_id, lock_t* lock_obj) {
    lock_obj->next_trx_lock_obj = trx_table[trx_id];
    trx_table[trx_id] = lock_obj;
}
void TrxManager::update_graph(lock_t* lock_obj) {
    // find preceding lock
    if(trx_adj.find(lock_obj->owner_trx_id) == trx_adj.end())
        trx_adj.insert({lock_obj->owner_trx_id, {}});

    lock_t* cur_lock_obj = lock_obj->prev;
    while(cur_lock_obj != lock_obj->sentinel->head) {
        if(cur_lock_obj->owner_trx_id == lock_obj->owner_trx_id
        || cur_lock_obj->record_id != lock_obj->record_id) {
            cur_lock_obj = cur_lock_obj->prev;
            continue;
        }

        if(cur_lock_obj->lock_mode == EXCLUSIVE_LOCK) {
            trx_adj[lock_obj->owner_trx_id].insert(cur_lock_obj->owner_trx_id);
            break;
        }
        else {
            if(lock_obj->lock_mode == EXCLUSIVE_LOCK) {
                trx_adj[lock_obj->owner_trx_id].insert(cur_lock_obj->owner_trx_id);
            }
        }

        cur_lock_obj = cur_lock_obj->prev;
    }
}
void TrxManager::add_log_to_trx(int64_t table_id, pagenum_t page_id, slotnum_t slot_num, int trx_id) {
    pthread_mutex_lock(&trx_manager_latch);

    char* old_value = nullptr;
    int old_val_size;

    buffer_t* page = buffer_manager.buffer_read_page(table_id, page_id);
    slotnum_t offset = page_io::leaf::get_offset((page_t*)page->frame, slot_num);
    old_val_size = page_io::leaf::get_record_size((page_t*)page->frame, slot_num);
    old_value = new char[old_val_size];
    page_io::leaf::get_record((page_t*)page->frame, offset, old_value, old_val_size);
    buffer_manager.unpin_buffer(table_id, page_id);

    trx_log_table[trx_id].push({std::string(old_value, old_val_size), old_val_size, table_id, page_id, slot_num});
    delete[] old_value;

    pthread_mutex_unlock(&trx_manager_latch);
}
void TrxManager::print_adj() {
    std::cout << "print adj" << std::endl;
    for(auto& node : trx_adj) {
        std::cout << "trx_id: " << node.first << " -> ";
        for(auto& adj_trx_id : node.second) {
            std::cout << adj_trx_id << " ";
        }
        std::cout << std::endl;
    }
    std::cout << "print adj end" << std::endl;
}

int trx_begin() {
    pthread_mutex_lock(&trx_manager_latch);

    int trx_id = ++global_trx_id;
    trx_manager.start_trx(trx_id);

    begin_log_t* log = new begin_log_t(trx_id);
    log_buf_manager.add_log(log);

    pthread_mutex_unlock(&trx_manager_latch);
    return trx_id;
}

int trx_commit(int trx_id) {
    pthread_mutex_lock(&trx_manager_latch);

    trx_manager.remove_trx(trx_id);

    commit_log_t* log = new commit_log_t(trx_id);
    log_buf_manager.add_log(log);

    pthread_mutex_unlock(&trx_manager_latch);

    return trx_id;
}

int trx_get_lock(int64_t table_id, pagenum_t page_id, slotnum_t slot_num, int trx_id, int lock_mode) {
    lock_t* lock_obj = lock_acquire(table_id, page_id, slot_num, trx_id, lock_mode);

    // transaction already has a lock on the record.
    if(lock_obj == nullptr) return 0;
    
    pthread_mutex_lock(&trx_manager_latch);

    trx_manager.add_action(trx_id, lock_obj);
    trx_manager.update_graph(lock_obj);

    if(trx_manager.is_deadlock(trx_id)) {
        //trx_manager.print_adj();
        return -1;
    }

    while(is_conflict(lock_obj)) {
        pthread_cond_wait(&lock_obj->cond, &trx_manager_latch);
    }

    pthread_mutex_unlock(&trx_manager_latch);
    return 0;
}

/* Newly Implemented Functions For Recovery (Project 6) */
int trx_abort(int trx_id) {
    pthread_mutex_lock(&trx_manager_latch);

    /* Latch Released in abort_trx() */
    trx_manager.abort_trx(trx_id);

    return trx_id;
}