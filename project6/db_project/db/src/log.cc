#include "log.h"

/* Global Log Buffer Manager Instance */
LogBufferManager log_buffer_manager;

/* Log Buffer Manager Latch */
pthread_mutex_t log_buffer_manager_latch;

/* Base Log Definition */
void log_t::write_log(int fd) {
    write(fd, &log_size, sizeof(log_size));
    write(fd, &LSN, sizeof(LSN));
    write(fd, &prev_LSN, sizeof(prev_LSN));
    write(fd, &trx_id, sizeof(trx_id));
    write(fd, &log_type, sizeof(log_type));
}

/* Begin Log Definition */
begin_log_t::begin_log_t() {
    log_size = DEFAULT_LOG_SIZE;
    LSN = -1;
    prev_LSN = -1;
    trx_id = -1;
    log_type = BEGIN_LOG;
}
void begin_log_t::write_log(int fd) {
    log_t::write_log(fd);
    sync();
}   

/* Commit Log Definition */
commit_log_t::commit_log_t() {
    log_size = DEFAULT_LOG_SIZE;
    LSN = -1;
    prev_LSN = -1;
    trx_id = -1;
    log_type = COMMIT_LOG;
}
void commit_log_t::write_log(int fd) {
    log_t::write_log(fd);
    sync(); 
}

/* Rollback Log Definition */
rollback_log_t::rollback_log_t() {
    log_size = DEFAULT_LOG_SIZE;
    LSN = -1;
    prev_LSN = -1;
    trx_id = -1;
    log_type = ROLLBACK_LOG;
}
void rollback_log_t::write_log(int fd) {
    log_t::write_log(fd);
    sync();
}

/* Update Log Definition */
update_log_t::update_log_t() {
    log_size = DEFAULT_UPDATE_LOG_SIZE;
    LSN = -1;
    prev_LSN = -1;
    trx_id = -1;
    log_type = UPDATE_LOG;
    table_id = -1;
    page_id = -1;
    offset = -1;
    data_length = -1;
    old_image = "";
    new_image = "";
}
void update_log_t::write_log(int fd) {
    log_t::write_log(fd);
    write(fd, &table_id, sizeof(table_id));
    write(fd, &page_id, sizeof(page_id));
    write(fd, &offset, sizeof(offset));
    write(fd, &data_length, sizeof(data_length));
    write(fd, old_image.c_str(), old_image.length());
    write(fd, new_image.c_str(), new_image.length());
    sync();
}

/* Compensate Log Definition */
compensate_log_t::compensate_log_t() {
    log_size = DEFAULT_COMPENSATE_LOG_SIZE;
    LSN = -1;
    prev_LSN = -1;
    trx_id = -1;
    log_type = COMPENSATE_LOG;
    table_id = -1;
    page_id = -1;
    offset = -1;
    data_length = -1;
    old_image = "";
    new_image = "";
    next_undo_LSN = -1;
}
void compensate_log_t::write_log(int fd) {
    log_t::write_log(fd);
    write(fd, &table_id, sizeof(table_id));
    write(fd, &page_id, sizeof(page_id));
    write(fd, &offset, sizeof(offset));
    write(fd, &data_length, sizeof(data_length));
    write(fd, old_image.c_str(), old_image.length());
    write(fd, new_image.c_str(), new_image.length());
    write(fd, &next_undo_LSN, sizeof(next_undo_LSN));
    sync();
}

/* Log Buffer Manager Definition */
LogBufferManager::LogBufferManager() {
    max_size = 0;
    log_fd = -1;
}
void LogBufferManager::init_buf(int buf_size) {
    max_size = buf_size;
}
void LogBufferManager::add_log(log_t* log) {
    pthread_mutex_lock(&log_buffer_manager_latch);

    if(log_buf.size() == max_size) {
        std::cerr << "log buffer has reached its maximum size" << std::endl;
        std::cerr << "exit DBMS..." << std::endl;
        pthread_mutex_unlock(&log_buffer_manager_latch);
        exit(-1);
    }

    log_buf.push_back(log);

    pthread_mutex_unlock(&log_buffer_manager_latch);
}
void LogBufferManager::flush_logs() {
    pthread_mutex_lock(&log_buffer_manager_latch);

    for(int i = 0; i < log_buf.size(); i++)
        log_buf[i]->write_log(log_fd);
    log_buf.clear();

    pthread_mutex_unlock(&log_buffer_manager_latch);
}