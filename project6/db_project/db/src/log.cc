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
    prev_LSN = 0;
    trx_id = -1;
    log_type = BEGIN_LOG;
}
begin_log_t::begin_log_t(int trx_id) {
    log_size = DEFAULT_LOG_SIZE;
    LSN = -1;
    prev_LSN = 0;
    this->trx_id = trx_id;
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
    prev_LSN = 0;
    trx_id = -1;
    log_type = COMMIT_LOG;
}
commit_log_t::commit_log_t(int trx_id) {
    log_size = DEFAULT_LOG_SIZE;
    LSN = -1;
    prev_LSN = 0;
    this->trx_id = trx_id;
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
    prev_LSN = 0;
    trx_id = -1;
    log_type = ROLLBACK_LOG;
}
rollback_log_t::rollback_log_t(int trx_id) {
    log_size = DEFAULT_LOG_SIZE;
    this->LSN = LSN;
    this->prev_LSN = prev_LSN;
    this->trx_id = trx_id;
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
    prev_LSN = 0;
    trx_id = -1;
    log_type = UPDATE_LOG;
    table_id = -1;
    page_id = -1;
    offset = -1;
    data_length = -1;
    old_image = "";
    new_image = "";
}
update_log_t::update_log_t(int trx_id, uint64_t LSN, uint64_t prev_LSN, uint64_t table_id, uint64_t page_id, slotnum_t offset, uint16_t data_length, std::string old_image, std::string new_image) {
    log_size = DEFAULT_UPDATE_LOG_SIZE + old_image.length() + new_image.length();
    this->LSN = LSN;
    this->prev_LSN = prev_LSN;
    this->trx_id = trx_id;
    log_type = UPDATE_LOG;
    this->table_id = table_id;
    this->page_id = page_id;
    this->offset = offset;
    this->data_length = data_length;
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
void update_log_t::add_old_image(std::string old_img) {
    old_image = old_img;
    log_size += old_image.length();
}
void update_log_t::add_new_image(std::string new_img) {
    new_image = new_img;
    log_size += new_image.length();
}

/* Compensate Log Definition */
compensate_log_t::compensate_log_t() {
    log_size = DEFAULT_COMPENSATE_LOG_SIZE;
    LSN = -1;
    prev_LSN = 0;
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
compensate_log_t::compensate_log_t(int trx_id, uint64_t LSN, uint64_t prev_LSN, uint64_t table_id, uint64_t page_id, slotnum_t offset, uint16_t data_length, std::string old_image, std::string new_image, uint64_t next_undo_LSN) {
    log_size = DEFAULT_COMPENSATE_LOG_SIZE + old_image.length() + new_image.length();
    this->LSN = LSN;
    this->prev_LSN = prev_LSN;
    this->trx_id = trx_id;
    log_type = COMPENSATE_LOG;
    this->table_id = table_id;
    this->page_id = page_id;
    this->offset = offset;
    this->data_length = data_length;
    this->next_undo_LSN = next_undo_LSN;
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
void compensate_log_t::add_old_image(std::string old_img) {
    old_image = old_img;
    log_size += old_image.length();
}
void compensate_log_t::add_new_image(std::string new_img) {
    new_image = new_img;
    log_size += new_image.length();
}

/************************************************************************/
// * LOG BUFFER MANAGER                                                 //
/************************************************************************/

/* Log Buffer Manager Definition */
LogBufferManager::LogBufferManager() {
    next_LSN = 0;
    max_size = 0;
    log_path = nullptr;
    logmsg_path = nullptr;
}
void LogBufferManager::init(int buf_size, char* log_path, char* logmsg_path) {
    max_size = buf_size;
    this->log_path = new char[strlen(log_path) + 1];
    this->logmsg_path = new char[strlen(logmsg_path) + 1];
    strcpy(this->log_path, log_path);
    strcpy(this->logmsg_path, logmsg_path);

    log_file_fd = open(log_path, O_RDWR | O_CREAT | O_SYNC | O_APPEND, 0644);
    logmsg_file = fopen(logmsg_path, "a+");
}
void LogBufferManager::add_log(log_t* log) {
    pthread_mutex_lock(&log_buffer_manager_latch);

    if(log_buf.size() == max_size) 
        flush_logs();

    next_LSN += log->log_size;
    log->LSN = next_LSN;
    log->prev_LSN = trx_manager.trx_last_LSN[log->trx_id];

    log_buf.push_back(log);

    pthread_mutex_unlock(&log_buffer_manager_latch);
}
void LogBufferManager::flush_logs() {
    for(int i = 0; i < log_buf.size(); i++) {
        log_buf[i]->write_log(log_file_fd);
        delete log_buf[i];
    }
    log_buf.clear();
}
/**
 * @brief read log to analyze it has been crashed or not
 * @return int (0 : not crashed, 1 : crashed)
 */
int LogBufferManager::analyze_log() {
    fprintf(logmsg_file, "[ANALYSIS] Analysis pass start\n");

    off_t cur_offset = 0;
    off_t end_offset = lseek(log_file_fd, 0, SEEK_END);
    lseek(log_file_fd, 0, SEEK_SET);

    std::set<int> trx_set;

    while(cur_offset != end_offset) {
        uint32_t log_size;
        uint32_t log_type;
        pread(log_file_fd, &log_size, sizeof(log_size), cur_offset);
        pread(log_file_fd, &log_type, sizeof(log_type), cur_offset + sizeof(uint32_t) * 3 + sizeof(uint64_t) * 2);

        char* tmp_buf = new char[log_size];
        pread(log_file_fd, tmp_buf, log_size, cur_offset);

        /* IN ANLYZE LOG, WE ONLY CONSIDER BEGIN, COMMIT, ROLLBACK LOG */

        // trx begin
        if(log_type == BEGIN_LOG) {
            begin_log_t* begin_log = (begin_log_t*)tmp_buf;
            trx_set.insert(begin_log->trx_id);
        }
        // trx commit
        else if(log_type == COMMIT_LOG) {
            commit_log_t* commit_log = (commit_log_t*)tmp_buf;
            trx_set.erase(commit_log->trx_id);
            win_trx.insert(commit_log->trx_id);
        }
        // trx abort
        else if(log_type == ROLLBACK_LOG) {
            rollback_log_t* rollback_log = (rollback_log_t*)tmp_buf;
            trx_set.erase(rollback_log->trx_id);
            win_trx.insert(rollback_log->trx_id);
        }

        delete[] tmp_buf;

        cur_offset += log_size;
    }

    for(const int& trx_id : trx_set) 
        lose_trx.insert(trx_id);

    fprintf(logmsg_file, "[ANALYSIS] Analysis success. Winner:");
    for(const int& trx_id : win_trx) 
        fprintf(logmsg_file, " %d", trx_id);
    fprintf(logmsg_file, ", Loser:");
    for(const int& trx_id : lose_trx) 
        fprintf(logmsg_file, " %d", trx_id);
    fprintf(logmsg_file, "\n");

    return (trx_set.empty()) ? 0 : 1;
}
/* this implementation is currently no consider-redo version. */
void LogBufferManager::redo_pass() {
    fprintf(logmsg_file, "[REDO] Redo pass start\n");

    off_t cur_offset = 0;
    off_t end_offset = lseek(log_file_fd, 0, SEEK_END);
    lseek(log_file_fd, 0, SEEK_SET);

    while(cur_offset != end_offset) {
        uint32_t log_size;
        uint32_t log_type;
        pread(log_file_fd, &log_size, sizeof(log_size), cur_offset);
        pread(log_file_fd, &log_type, sizeof(log_type), cur_offset + sizeof(uint32_t) * 3 + sizeof(uint64_t) * 2);

        char* tmp_buf = new char[log_size];
        pread(log_file_fd, tmp_buf, log_size, cur_offset);
        delete[] tmp_buf;

        if(log_type == BEGIN_LOG || log_type == COMMIT_LOG || log_type == ROLLBACK_LOG) {
            if(log_type == BEGIN_LOG) fprintf(logmsg_file, "LSN %lu [BEGIN] Transaction id %d\n", ((begin_log_t*)tmp_buf)->LSN, ((begin_log_t*)tmp_buf)->trx_id);
            else if(log_type == COMMIT_LOG) fprintf(logmsg_file, "LSN %lu [COMMIT] Transaction id %d\n", ((commit_log_t*)tmp_buf)->LSN, ((commit_log_t*)tmp_buf)->trx_id);
            else if(log_type == ROLLBACK_LOG) fprintf(logmsg_file, "LSN %lu [ROLLBACK] Transaction id %d\n", ((rollback_log_t*)tmp_buf)->LSN, ((rollback_log_t*)tmp_buf)->trx_id);
            cur_offset += log_size;
            continue;
        }

        if(log_type == UPDATE_LOG) {
            update_log_t* update_log = (update_log_t*)tmp_buf;

            // CLR update here
            // if(lose_trx.find(update_log->trx_id) != lose_trx.end()) {
            //     compensate_log_t* clr = new compensate_log_t;
                
            //     /* set compensate log */


            // }

            buffer_t* buf = buffer_manager.buffer_read_page(update_log->table_id, update_log->page_id);
            buffer_manager.buffer_write_page(update_log->table_id, update_log->page_id);
            page_io::leaf::set_record((page_t*)buf->frame, update_log->offset, update_log->new_image.c_str(), update_log->new_image.size());
            buffer_manager.unpin_buffer(update_log->table_id, update_log->page_id);
        }
        else if(log_type == COMPENSATE_LOG) {
            compensate_log_t* compensate_log = (compensate_log_t*)tmp_buf;
            buffer_t* buf = buffer_manager.buffer_read_page(compensate_log->table_id, compensate_log->page_id);
            buffer_manager.buffer_write_page(compensate_log->table_id, compensate_log->page_id);
            page_io::leaf::set_record((page_t*)buf->frame, compensate_log->offset, compensate_log->new_image.c_str(), compensate_log->new_image.size());
            buffer_manager.unpin_buffer(compensate_log->table_id, compensate_log->page_id);
        }

        cur_offset += log_size;
    }

}
void LogBufferManager::undo_pass() {
    

}
void LogBufferManager::recovery() {
    pthread_mutex_lock(&log_buffer_manager_latch);

    int flag = analyze_log();
    if(flag == 0) {
        pthread_mutex_unlock(&log_buffer_manager_latch);
        return;
    }

    redo_pass();
    undo_pass();
    
    // force flush
    flush_logs();
    fflush(logmsg_file);

    win_trx = {}, lose_trx = {};

    pthread_mutex_unlock(&log_buffer_manager_latch);
}