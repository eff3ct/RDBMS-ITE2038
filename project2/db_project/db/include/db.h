/* Declarations of the API functions */
#ifndef DB_H
#define DB_H

#include <stdint.h>

#include <vector>

/* Open existing data file using ‘pathname’ or create one if not existed. 
* If success, return a unique table id else return negative value.
*/
int64_t open_table(const char* pathname);

/* Insert key/value pair to data file.
* If success, return 0 else return non-zero value.
*/
int db_insert(int64_t table_id, int64_t key, const char* value, uint16_t val_size);

/* Find a record containing the 'key'.
* If a matching key exists, store its value in 'ret_val' and the corresponding size in 'val_size'.
* If success, return 0 else return non-zero value.
*/
int db_find(int64_t table_id, int64_t key, char* ret_val, uint16_t* val_size);

/* Find a record with the matching key and delete it.
* If success, return 0 else return non-zero value.
*/
int db_delete(int64_t table_id, int64_t key);

/* Find records with keys in the range of [start_key, end_key].
* If success, return 0 else return non-zero value.
*/
int db_scan(int64_t table_id, int64_t begin_key, int64_t end_key,
std::vector<int64_t>* keys, std::vector<char*>* values, std::vector<uint16_t>* val_sizes);

/* Initialize DBMS.
* If success, return 0 else return non-zero value.
*/
int init_db();

/* Shutdown DBMS.
* If success, return 0 else return non-zero value.
*/
int shutdown_db();

#endif