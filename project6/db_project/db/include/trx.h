#ifndef TRX_H
#define TRX_H

#include <stdint.h>
#include <string.h>
#include <pthread.h>

#include <unordered_map>
#include <stack>
#include <vector>
#include <functional>
#include <algorithm>
#include <string>
#include <set>
#include <map>

#include "lock_table.h"
#include "log.h"
#include "buffer.h"

#define SHARED_LOCK 0
#define EXCLUSIVE_LOCK 1

// Transaction Manager Latch
extern pthread_mutex_t trx_manager_latch;

// Global Transaction ID 
extern int64_t global_trx_id;

// Transaction Manager
class TrxManager {
    private:
        struct log_t {
            std::string old_value;
            int old_val_size;
            int64_t table_id;
            pagenum_t page_id;
            slotnum_t slot_num;
        };
        
        std::unordered_map<int, lock_t*> trx_table;
        std::map<int, std::set<int>> trx_adj;
        std::map<int, std::stack<log_t>> trx_log_table;

        void remove_trx_node(int trx_id);
        void undo_actions(int trx_id);
        
    public:
        // initialize transaction manager
        void init();
        // print adj
        void print_adj();
        // add log
        void add_log_to_trx(int64_t table_id, pagenum_t page_id, slotnum_t slot_num, int trx_id);
        // update wait for graph
        void update_graph(lock_t* lock);
        // check cycle
        bool is_deadlock(int trx_id);
        // Add transaction to trx_table
        void start_trx(int trx_id);
        // Add action on trx_id
        void add_action(int trx_id, lock_t* lock_obj);
        // Remove transaction from trx_table
        void remove_trx(int trx_id);
        // Abort transaction
        void abort_trx(int trx_id);
};

// Transaction Manager Instanace
extern TrxManager trx_manager;

/**
 * Allocate a transaction structure and initialize it.
 * If success, return a unique transaction id else return zero.
 */
int trx_begin();

/**
 * Commit a transaction.
 * If success, return trx_id, else return zero.
 * @param trx_id Transaction id to commit.
 */
int trx_commit(int trx_id);

/**
 * Abort a transaction.
 * If success, return trx_id, else return zero.
 * @param trx_id Transaction id to abort.
 */
int trx_abort(int trx_id);

int trx_get_lock(int64_t table_id, pagenum_t page_id, slotnum_t slot_num, int trx_id, int lock_mode);

#endif