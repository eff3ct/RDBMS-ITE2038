/* Declarations of the API functions */
#ifndef DB_H
#define DB_H

#include "buffer.h"
#include "on-disk-bpt.h"
#include "trx.h"
#include "lock_table.h"
#include "log.h"

#include <stdint.h>

#include <iostream>
#include <set>
#include <vector>

/** Open existing data file using ‘pathname’ or create one if not existed. 
 * If success, return a unique table id else return negative value.
 */
int64_t open_table(const char* pathname);

/** Insert key/value pair to data file.
 * If success, return 0 else return non-zero value.
 */
int db_insert(int64_t table_id, int64_t key, const char* value, uint16_t val_size);

/** Find a record containing the 'key'.
 * If a matching key exists, store its value in 'ret_val' and the corresponding size in 'val_size'.
 * If success, return 0 else return non-zero value.
 */
int db_find(int64_t table_id, int64_t key, char* ret_val, uint16_t* val_size);

/** Find a record containing the 'key'.
 * If a matching key exists, store its value in 'ret_val' and the corresponding size in 'val_size'.
 * If success, return 0 else return non-zero value.
 * * support transaction
 */
int db_find(int64_t table_id, int64_t key, char* ret_val, uint16_t* val_size, int trx_id);

/** Find a record with the matching key and delete it.
 * If success, return 0 else return non-zero value.
 */
int db_delete(int64_t table_id, int64_t key);

/** Find records with keys in the range of [start_key, end_key].
 * If success, return 0 else return non-zero value.
 */
int db_scan(int64_t table_id, int64_t begin_key, int64_t end_key,
std::vector<int64_t>* keys, std::vector<char*>* values, std::vector<uint16_t>* val_sizes);

/** Update a record containing the 'key'.
 * If a matching key exists, update its value with 'value'.
 * If success, return 0 else return non-zero value and the transacntion has to be aborted.
 */
int db_update(int64_t table_id, int64_t key, char* value, uint16_t new_val_size, uint16_t* old_val_size, int trx_id);

/** Initialize DBMS.
 * If success, return 0 else return non-zero value.
 */
int init_db(int num_buf);

/** Initialize DBMS Project 6
 * If success, return 0 else return non-zero value.
 */
int init_db(int buf_num, int flag, int log_num, char* log_path, char* logmsg_path);

/** Shutdown DBMS.
 * If success, return 0 else return non-zero value.
 */
int shutdown_db();

#endif