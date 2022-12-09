#include "log.h"

/* Begin Log Definition */
begin_log_t::begin_log_t() {
    log_size = sizeof(begin_log_t);
    LSN = -1;
    prev_LSN = -1;
    trx_id = -1;
    log_type = -1;
}
void begin_log_t::write_log(int fd) {
    
}   

/* Commit Log Definition */
commit_log_t::commit_log_t() {
    log_size = sizeof(commit_log_t);
    LSN = -1;
    prev_LSN = -1;
    trx_id = -1;
    log_type = -1;
}

/* Rollback Log Definition */
rollback_log_t::rollback_log_t() {
    log_size = sizeof(rollback_log_t);
    LSN = -1;
    prev_LSN = -1;
    trx_id = -1;
    log_type = -1;
}

/* Update Log Definition */
update_log_t::update_log_t() {
    log_size = sizeof(update_log_t);
    LSN = -1;
    prev_LSN = -1;
    trx_id = -1;
    log_type = -1;
    table_id = -1;
    page_id = -1;
    offset = -1;
    data_length = -1;
    old_image = "";
    new_image = "";
}

/* Compensate Log Definition */
compensate_log_t::compensate_log_t() {
    log_size = sizeof(compensate_log_t);
    LSN = -1;
    prev_LSN = -1;
    trx_id = -1;
    log_type = -1;
    table_id = -1;
    page_id = -1;
    offset = -1;
    data_length = -1;
    old_image = "";
    new_image = "";
    next_undo_LSN = -1;
}