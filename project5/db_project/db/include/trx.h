#ifndef TRX_H
#define TRX_H

#include <stdint.h>
#include <string.h>
#include <pthread.h>
#include <unordered_map>

#include "lock_table.h"

#define SHARED_LOCK 0
#define EXCLUSIVE_LOCK 1

// Transaction Manager Latch
extern pthread_mutex_t trx_manager_latch;

// Global Transaction ID 
extern int global_trx_id;

// Transaction Manager
class TrxManager {
    private:
        std::unordered_map<int, lock_t*> trx_table;
        
    public:
        // Constructor
        TrxManager();
        // Add transaction to trx_table
        void start_trx(int trx_id);
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

#endif TRX_H