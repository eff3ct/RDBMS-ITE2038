#include "log.h"

/* last LSN of each transaction */
std::unordered_map<int, uint64_t> trx_last_LSN;

extern BufferManager buffer_manager;

/* Global Log Buffer Manager Instance */
LogBufferManager log_buf_manager;

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
update_log_t::update_log_t(int trx_id, uint64_t table_id, uint64_t page_id, slotnum_t offset, uint16_t data_length, std::string old_image, std::string new_image) {
    log_size = DEFAULT_UPDATE_LOG_SIZE + old_image.length() + new_image.length();
    this->LSN = -1;
    this->prev_LSN = 0;
    this->trx_id = trx_id;
    log_type = UPDATE_LOG;
    this->table_id = table_id;
    this->page_id = page_id;
    this->offset = offset;
    this->data_length = data_length;
    this->old_image = old_image;
    this->new_image = new_image;
}
void update_log_t::write_log(int fd) {
    log_t::write_log(fd);
    write(fd, &table_id, sizeof(table_id));
    write(fd, &page_id, sizeof(page_id));
    write(fd, &offset, sizeof(offset));
    write(fd, &data_length, sizeof(data_length));
    write(fd, (const char*)old_image.c_str(), old_image.length());
    write(fd, (const char*)new_image.c_str(), new_image.length());
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
compensate_log_t::compensate_log_t(int trx_id, uint64_t table_id, uint64_t page_id, slotnum_t offset, uint16_t data_length, std::string old_image, std::string new_image, uint64_t next_undo_LSN) {
    log_size = DEFAULT_COMPENSATE_LOG_SIZE + old_image.length() + new_image.length();
    this->LSN = -1;
    this->prev_LSN = 0;
    this->trx_id = trx_id;
    log_type = COMPENSATE_LOG;
    this->table_id = table_id;
    this->page_id = page_id;
    this->offset = offset;
    this->data_length = data_length;
    this->next_undo_LSN = next_undo_LSN;
    this->old_image = old_image;
    this->new_image = new_image;
}
void compensate_log_t::write_log(int fd) {
    log_t::write_log(fd);
    write(fd, &table_id, sizeof(table_id));
    write(fd, &page_id, sizeof(page_id));
    write(fd, &offset, sizeof(offset));
    write(fd, &data_length, sizeof(data_length));
    write(fd, (const char*)old_image.c_str(), old_image.length());
    write(fd, (const char*)new_image.c_str(), new_image.length());
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

    log->LSN = next_LSN;
    log->prev_LSN = trx_last_LSN[log->trx_id];
    trx_last_LSN[log->trx_id] = log->LSN;
    next_LSN += log->log_size;

    log_buf.push_back(log);

    pthread_mutex_unlock(&log_buffer_manager_latch);
}
void LogBufferManager::add_log_no_latch(log_t* log) {
    if(log_buf.size() == max_size) 
        flush_logs();

    log->LSN = next_LSN;
    log->prev_LSN = trx_last_LSN[log->trx_id];
    trx_last_LSN[log->trx_id] = log->LSN;
    next_LSN += log->log_size;

    log_buf.push_back(log);
}
void LogBufferManager::flush_logs() {
    for(int i = 0; i < log_buf.size(); i++) {
        log_buf[i]->write_log(log_file_fd);
        delete log_buf[i];
    }
    log_buf = {};
}
begin_log_t LogBufferManager::make_begin_log(char* buf) {
    begin_log_t log;
    memcpy(&log.log_size, buf, sizeof(log.log_size));
    memcpy(&log.LSN, buf + sizeof(log.log_size), sizeof(log.LSN));
    memcpy(&log.prev_LSN, buf + sizeof(log.log_size) + sizeof(log.LSN), sizeof(log.prev_LSN));
    memcpy(&log.trx_id, buf + sizeof(log.log_size) + sizeof(log.LSN) + sizeof(log.prev_LSN), sizeof(log.trx_id));
    memcpy(&log.log_type, buf + sizeof(log.log_size) + sizeof(log.LSN) + sizeof(log.prev_LSN) + sizeof(log.trx_id), sizeof(log.log_type));
    return log;
}
commit_log_t LogBufferManager::make_commit_log(char* buf) {
    commit_log_t log;
    memcpy(&log.log_size, buf, sizeof(log.log_size));
    memcpy(&log.LSN, buf + sizeof(log.log_size), sizeof(log.LSN));
    memcpy(&log.prev_LSN, buf + sizeof(log.log_size) + sizeof(log.LSN), sizeof(log.prev_LSN));
    memcpy(&log.trx_id, buf + sizeof(log.log_size) + sizeof(log.LSN) + sizeof(log.prev_LSN), sizeof(log.trx_id));
    memcpy(&log.log_type, buf + sizeof(log.log_size) + sizeof(log.LSN) + sizeof(log.prev_LSN) + sizeof(log.trx_id), sizeof(log.log_type));
    return log;
}
rollback_log_t LogBufferManager::make_rollback_log(char* buf) {
    rollback_log_t log;
    memcpy(&log.log_size, buf, sizeof(log.log_size));
    memcpy(&log.LSN, buf + sizeof(log.log_size), sizeof(log.LSN));
    memcpy(&log.prev_LSN, buf + sizeof(log.log_size) + sizeof(log.LSN), sizeof(log.prev_LSN));
    memcpy(&log.trx_id, buf + sizeof(log.log_size) + sizeof(log.LSN) + sizeof(log.prev_LSN), sizeof(log.trx_id));
    memcpy(&log.log_type, buf + sizeof(log.log_size) + sizeof(log.LSN) + sizeof(log.prev_LSN) + sizeof(log.trx_id), sizeof(log.log_type));
    return log;
}
update_log_t LogBufferManager::make_update_log(char* buf) {
    update_log_t log;
    memcpy(&log.log_size, buf, sizeof(log.log_size));
    memcpy(&log.LSN, buf + sizeof(log.log_size), sizeof(log.LSN));
    memcpy(&log.prev_LSN, buf + sizeof(log.log_size) + sizeof(log.LSN), sizeof(log.prev_LSN));
    memcpy(&log.trx_id, buf + sizeof(log.log_size) + sizeof(log.LSN) + sizeof(log.prev_LSN), sizeof(log.trx_id));
    memcpy(&log.log_type, buf + sizeof(log.log_size) + sizeof(log.LSN) + sizeof(log.prev_LSN) + sizeof(log.trx_id), sizeof(log.log_type));
    
    memcpy(&log.table_id, buf + sizeof(log.log_size) + sizeof(log.LSN) + sizeof(log.prev_LSN) + sizeof(log.trx_id) + sizeof(log.log_type), sizeof(log.table_id));
    memcpy(&log.page_id, buf + sizeof(log.log_size) + sizeof(log.LSN) + sizeof(log.prev_LSN) + sizeof(log.trx_id) + sizeof(log.log_type) + sizeof(log.table_id), sizeof(log.page_id));
    memcpy(&log.offset, buf + sizeof(log.log_size) + sizeof(log.LSN) + sizeof(log.prev_LSN) + sizeof(log.trx_id) + sizeof(log.log_type) + sizeof(log.table_id) + sizeof(log.page_id), sizeof(log.offset));
    memcpy(&log.data_length, buf + sizeof(log.log_size) + sizeof(log.LSN) + sizeof(log.prev_LSN) + sizeof(log.trx_id) + sizeof(log.log_type) + sizeof(log.table_id) + sizeof(log.page_id) + sizeof(log.offset), sizeof(log.data_length));
    char* tmp_dat = new char[log.data_length];
    memcpy(tmp_dat, buf + sizeof(log.log_size) + sizeof(log.LSN) + sizeof(log.prev_LSN) + sizeof(log.trx_id) + sizeof(log.log_type) + sizeof(log.table_id) + sizeof(log.page_id) + sizeof(log.offset) + sizeof(log.data_length), log.data_length);
    log.old_image = std::string(tmp_dat, log.data_length);
    memcpy(tmp_dat, buf + sizeof(log.log_size) + sizeof(log.LSN) + sizeof(log.prev_LSN) + sizeof(log.trx_id) + sizeof(log.log_type) + sizeof(log.table_id) + sizeof(log.page_id) + sizeof(log.offset) + sizeof(log.data_length) + log.data_length, log.data_length);
    log.new_image = std::string(tmp_dat, log.data_length);
    delete[] tmp_dat;
    return log;
}
compensate_log_t LogBufferManager::make_compensate_log(char* buf) {
    compensate_log_t log;
    memcpy(&log.log_size, buf, sizeof(log.log_size));
    memcpy(&log.LSN, buf + sizeof(log.log_size), sizeof(log.LSN));
    memcpy(&log.prev_LSN, buf + sizeof(log.log_size) + sizeof(log.LSN), sizeof(log.prev_LSN));
    memcpy(&log.trx_id, buf + sizeof(log.log_size) + sizeof(log.LSN) + sizeof(log.prev_LSN), sizeof(log.trx_id));
    memcpy(&log.log_type, buf + sizeof(log.log_size) + sizeof(log.LSN) + sizeof(log.prev_LSN) + sizeof(log.trx_id), sizeof(log.log_type));
    memcpy(&log.table_id, buf + sizeof(log.log_size) + sizeof(log.LSN) + sizeof(log.prev_LSN) + sizeof(log.trx_id) + sizeof(log.log_type), sizeof(log.table_id));
    memcpy(&log.page_id, buf + sizeof(log.log_size) + sizeof(log.LSN) + sizeof(log.prev_LSN) + sizeof(log.trx_id) + sizeof(log.log_type) + sizeof(log.table_id), sizeof(log.page_id));
    memcpy(&log.offset, buf + sizeof(log.log_size) + sizeof(log.LSN) + sizeof(log.prev_LSN) + sizeof(log.trx_id) + sizeof(log.log_type) + sizeof(log.table_id) + sizeof(log.page_id), sizeof(log.offset));
    memcpy(&log.data_length, buf + sizeof(log.log_size) + sizeof(log.LSN) + sizeof(log.prev_LSN) + sizeof(log.trx_id) + sizeof(log.log_type) + sizeof(log.table_id) + sizeof(log.page_id) + sizeof(log.offset), sizeof(log.data_length));
    char* tmp_dat = new char[log.data_length];
    memcpy(tmp_dat, buf + sizeof(log.log_size) + sizeof(log.LSN) + sizeof(log.prev_LSN) + sizeof(log.trx_id) + sizeof(log.log_type) + sizeof(log.table_id) + sizeof(log.page_id) + sizeof(log.offset) + sizeof(log.data_length), log.data_length);
    log.old_image = std::string(tmp_dat, log.data_length);
    memcpy(tmp_dat, buf + sizeof(log.log_size) + sizeof(log.LSN) + sizeof(log.prev_LSN) + sizeof(log.trx_id) + sizeof(log.log_type) + sizeof(log.table_id) + sizeof(log.page_id) + sizeof(log.offset) + sizeof(log.data_length) + log.data_length, log.data_length);
    log.new_image = std::string(tmp_dat, log.data_length);
    delete[] tmp_dat;
    memcpy(&log.next_undo_LSN, buf + sizeof(log.log_size) + sizeof(log.LSN) + sizeof(log.prev_LSN) + sizeof(log.trx_id) + sizeof(log.log_type) + sizeof(log.table_id) + sizeof(log.page_id) + sizeof(log.offset) + sizeof(log.data_length) + log.data_length * 2, sizeof(log.next_undo_LSN));
    return log;
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
        pread(log_file_fd, &log_type, sizeof(log_type), cur_offset + sizeof(uint32_t) * 2 + sizeof(uint64_t) * 2);

        char* tmp_buf = new char[log_size];
        pread(log_file_fd, tmp_buf, log_size, cur_offset);

        /* IN ANLYZE LOG, WE ONLY CONSIDER BEGIN, COMMIT, ROLLBACK LOG */

        // trx begin
        if(log_type == BEGIN_LOG) {
            begin_log_t begin_log = make_begin_log(tmp_buf);
            trx_set.insert(begin_log.trx_id);
            trx_last_LSN[begin_log.trx_id] = begin_log.LSN;
        }
        // trx commit
        else if(log_type == COMMIT_LOG) {
            commit_log_t commit_log = make_commit_log(tmp_buf);
            trx_set.erase(commit_log.trx_id);
            win_trx.insert(commit_log.trx_id);
            trx_last_LSN[commit_log.trx_id] = commit_log.LSN;
        }
        // trx abort
        else if(log_type == ROLLBACK_LOG) {
            rollback_log_t rollback_log = make_rollback_log(tmp_buf);
            trx_set.erase(rollback_log.trx_id);
            win_trx.insert(rollback_log.trx_id);
            trx_last_LSN[rollback_log.trx_id] = rollback_log.LSN;
        }
        // update
        else if(log_type == UPDATE_LOG) {
            update_log_t update_log = make_update_log(tmp_buf);
            trx_last_LSN[update_log.trx_id] = update_log.LSN;
        }
        // compensate
        else if(log_type == COMPENSATE_LOG) {
            compensate_log_t compensate_log = make_compensate_log(tmp_buf);
            trx_last_LSN[compensate_log.trx_id] = compensate_log.LSN;
        }

        delete[] tmp_buf;

        cur_offset += log_size;
    }

    next_LSN = cur_offset;

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
int LogBufferManager::redo_pass(int flag, int log_num) {
    int cur_log_cnt = 0;    

    std::set<int> table_set;

    fprintf(logmsg_file, "[REDO] Redo pass start\n");

    off_t cur_offset = 0;
    off_t end_offset = lseek(log_file_fd, 0, SEEK_END);
    lseek(log_file_fd, 0, SEEK_SET);

    while(cur_offset != end_offset) {
        if(flag == 1 && cur_log_cnt >= log_num) return 1;

        uint32_t log_size;
        uint32_t log_type;
        pread(log_file_fd, &log_size, sizeof(log_size), cur_offset);
        pread(log_file_fd, &log_type, sizeof(log_type), cur_offset + sizeof(uint32_t) * 2 + sizeof(uint64_t) * 2);

        char* tmp_buf = new char[log_size];
        pread(log_file_fd, tmp_buf, log_size, cur_offset);

        if(log_type == BEGIN_LOG || log_type == COMMIT_LOG || log_type == ROLLBACK_LOG) {
            cur_log_cnt++;
            if(log_type == BEGIN_LOG) {
                begin_log_t begin_log = make_begin_log(tmp_buf);
                fprintf(logmsg_file, "LSN %lu [BEGIN] Transaction id %d\n", begin_log.LSN, begin_log.trx_id);
            }
            else if(log_type == COMMIT_LOG) {
                commit_log_t commit_log = make_commit_log(tmp_buf);
                fprintf(logmsg_file, "LSN %lu [COMMIT] Transaction id %d\n", commit_log.LSN, commit_log.trx_id);
            }
            else if(log_type == ROLLBACK_LOG) {
                rollback_log_t rollback_log = make_rollback_log(tmp_buf);
                fprintf(logmsg_file, "LSN %lu [ROLLBACK] Transaction id %d\n", rollback_log.LSN, rollback_log.trx_id);
            }
            cur_offset += log_size;
            delete[] tmp_buf;
            continue;
        }

        if(log_type == UPDATE_LOG) {
            cur_log_cnt++;
            update_log_t update_log = make_update_log(tmp_buf);

            if(table_set.find(update_log.table_id) == table_set.end()) {
                std::string table_name = "DATA" + std::to_string(update_log.table_id);
                open_table((const char*)table_name.c_str());
                table_set.insert(update_log.table_id);
            }

            buffer_t* buf = buffer_manager.buffer_read_page(update_log.table_id, update_log.page_id);

            uint64_t page_LSN = page_io::get_page_LSN((page_t*)buf->frame);

            if(page_LSN >= update_log.LSN) {
                fprintf(logmsg_file, "LSN %ld [CONSIDER-REDO] Transaction id %d\n", update_log.LSN, update_log.trx_id);
                buffer_manager.unpin_buffer(update_log.table_id, update_log.page_id);

                cur_offset += log_size;
                delete[] tmp_buf;
                continue;
            }

            buffer_manager.buffer_write_page(update_log.table_id, update_log.page_id);
            page_io::set_page_LSN((page_t*)buf->frame, update_log.LSN);
            page_io::leaf::set_record((page_t*)buf->frame, update_log.offset, update_log.new_image.c_str(), update_log.new_image.size());
            buffer_manager.unpin_buffer(update_log.table_id, update_log.page_id);

            fprintf(logmsg_file, "LSN %lu [UPDATE] Transaction id %d redo apply\n", update_log.LSN, update_log.trx_id);
        }
        else if(log_type == COMPENSATE_LOG) {
            cur_log_cnt++;
            compensate_log_t compensate_log = make_compensate_log(tmp_buf);
            buffer_t* buf = buffer_manager.buffer_read_page(compensate_log.table_id, compensate_log.page_id);

            uint64_t page_LSN = page_io::get_page_LSN((page_t*)buf->frame);
            if(page_LSN >= compensate_log.LSN) {
                fprintf(logmsg_file, "LSN %lu [CONSIDER-REDO] Transaction id %d\n", compensate_log.LSN, compensate_log.trx_id);
                buffer_manager.unpin_buffer(compensate_log.table_id, compensate_log.page_id);

                cur_offset += log_size;
                delete[] tmp_buf;
                continue;
            }

            buffer_manager.buffer_write_page(compensate_log.table_id, compensate_log.page_id);
            page_io::set_page_LSN((page_t*)buf->frame, compensate_log.LSN);
            page_io::leaf::set_record((page_t*)buf->frame, compensate_log.offset, compensate_log.new_image.c_str(), compensate_log.new_image.size());
            buffer_manager.unpin_buffer(compensate_log.table_id, compensate_log.page_id);

            fprintf(logmsg_file, "LSN %lu [CLR] next undo lsn %lu\n", compensate_log.LSN, compensate_log.next_undo_LSN);
        }

        delete[] tmp_buf;
        cur_offset += log_size;
    }

    fprintf(logmsg_file, "[REDO] Redo pass end\n");

    return 0;
}
int LogBufferManager::undo_pass(int flag, int log_num) {
    int cur_log_cnt = 0;
    fprintf(logmsg_file, "[UNDO] Undo pass start\n");
    
    while(true) {
        uint64_t cur_LSN = 0;

        for(const int& cur_loser : lose_trx) {
            cur_LSN = std::max(cur_LSN, trx_last_LSN[cur_loser]);
        }

        if(cur_LSN == 0) break;

        uint32_t log_type;
        uint32_t log_size;
        pread(log_file_fd, &log_size, sizeof(uint32_t), cur_LSN);
        pread(log_file_fd, &log_type, sizeof(uint32_t), cur_LSN + sizeof(uint32_t) * 2 + sizeof(uint64_t) * 2);

        char* tmp_buf = new char[log_size];
        pread(log_file_fd, tmp_buf, log_size, cur_LSN);

        if(log_type == UPDATE_LOG) {
            cur_log_cnt++;

            update_log_t update_log = make_update_log(tmp_buf);
            buffer_t* buf = buffer_manager.buffer_read_page(update_log.table_id, update_log.page_id);

            uint64_t page_LSN = page_io::get_page_LSN((page_t*)buf->frame);
            if(page_LSN >= update_log.LSN) {
                fprintf(logmsg_file, "LSN %lu [UPDATE] Transaction id %d undo apply\n", update_log.LSN, update_log.trx_id);

                buffer_manager.buffer_write_page(update_log.table_id, update_log.page_id);

                compensate_log_t* compensate_log = new compensate_log_t(
                    update_log.trx_id,
                    update_log.table_id, 
                    update_log.page_id, 
                    update_log.offset, 
                    update_log.data_length,
                    update_log.new_image,
                    update_log.old_image,
                    update_log.prev_LSN
                );
                log_buf_manager.add_log_no_latch(compensate_log);

                page_io::set_page_LSN((page_t*)buf->frame, compensate_log->LSN);
                page_io::leaf::set_record((page_t*)buf->frame, update_log.offset, update_log.old_image.c_str(), update_log.old_image.size());
            }

            buffer_manager.unpin_buffer(update_log.table_id, update_log.page_id);

            trx_last_LSN[update_log.trx_id] = update_log.prev_LSN;
        }
        else if(log_type == BEGIN_LOG) {
            cur_log_cnt++;

            begin_log_t begin_log = make_begin_log(tmp_buf);
            fprintf(logmsg_file, "LSN %lu [BEGIN] Transaction id %d\n", begin_log.LSN, begin_log.trx_id);

            rollback_log_t* rollback_log = new rollback_log_t(begin_log.trx_id);
            log_buf_manager.add_log_no_latch(rollback_log);

            trx_last_LSN.erase(begin_log.trx_id);
            lose_trx.erase(begin_log.trx_id);
        }
        else if(log_type == COMPENSATE_LOG) {
            cur_log_cnt++;

            compensate_log_t compensate_log = make_compensate_log(tmp_buf);
            fprintf(logmsg_file, "LSN %lu [CLR] Transaction id %d\n", compensate_log.LSN, compensate_log.trx_id);
            if(compensate_log.next_undo_LSN == 0) {
                trx_last_LSN.erase(compensate_log.trx_id);
                lose_trx.erase(compensate_log.trx_id);
            }
            else
                trx_last_LSN[compensate_log.trx_id] = compensate_log.next_undo_LSN;
        }
        else if(log_type == COMMIT_LOG) {
            cur_log_cnt++;

            commit_log_t commit_log = make_commit_log(tmp_buf);
            fprintf(logmsg_file, "LSN %lu [COMMIT] Transaction id %d\n", commit_log.LSN, commit_log.trx_id);
        }
        else if(log_type == ROLLBACK_LOG) {
            cur_log_cnt++;

            rollback_log_t rollback_log = make_rollback_log(tmp_buf);
            fprintf(logmsg_file, "LSN %lu [ROLLBACK] Transaction id %d\n", rollback_log.LSN, rollback_log.trx_id);
        }

        delete[] tmp_buf;

        if(flag == 2 && cur_log_cnt >= log_num) return 1;
    }

    fprintf(logmsg_file, "[UNDO] Undo pass end\n");

    return 0;
}
void LogBufferManager::recovery(int flag, int log_num) {
    pthread_mutex_lock(&log_buffer_manager_latch);

    int flag_ = analyze_log();
    if(flag_ == 0) {
        // If no loser trx, truncate log file.
        ftruncate(log_file_fd, 0);
        next_LSN = 0;
        pthread_mutex_unlock(&log_buffer_manager_latch);
        return;
    }

    int redo_flag = redo_pass(flag, log_num);
    if(redo_flag == 1) {
        pthread_mutex_unlock(&log_buffer_manager_latch);
        return;
    }

    int undo_flag = undo_pass(flag, log_num);
    if(undo_flag == 1) {
        pthread_mutex_unlock(&log_buffer_manager_latch);
        return;
    }
    
    // force flush
    flush_logs();
    fflush(logmsg_file);

    win_trx = {}, lose_trx = {};

    pthread_mutex_unlock(&log_buffer_manager_latch);
}
void LogBufferManager::end_log() {
    pthread_mutex_lock(&log_buffer_manager_latch);
    flush_logs();
    fflush(logmsg_file);

    win_trx = {}, lose_trx = {};
    trx_last_LSN = {};

    fclose(logmsg_file); logmsg_file = nullptr;
    close(log_file_fd);
    
    max_size = 0;
    log_path = nullptr;
    logmsg_path = nullptr;

    next_LSN = 0;
    pthread_mutex_unlock(&log_buffer_manager_latch);
}
