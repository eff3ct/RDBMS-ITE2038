#ifndef LOG_H
#define LOG_H

#define BEGIN_LOG 0
#define UPDATE_LOG 1
#define COMMIT_LOG 2
#define ROLLBACK_LOG 3
#define COMPENSATE_LOG 4

#define DEFAULT_LOG_SIZE 28
#define DEFAULT_UPDATE_LOG_SIZE 48
#define DEFAULT_COMPENSATE_LOG_SIZE 56

#include "buffer.h"
#include "trx.h"

#include <string.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include <pthread.h>

#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>

#include <string>
#include <iostream>
#include <vector>
#include <set>

using slotnum_t = int16_t;

// Log Buffer Manager Latch
extern pthread_mutex_t log_buffer_manager_latch;

// Base Log Structure
class log_t {
    public:
        uint32_t log_size;
        uint64_t LSN;
        uint64_t prev_LSN;
        uint32_t trx_id;
        uint32_t log_type;

        virtual void write_log(int fd);
};

// Begin / Commit / Rollback Log
class begin_log_t : public log_t {
    public:
        begin_log_t();
        begin_log_t(int trx_id);

        void write_log(int fd) override;
};
class commit_log_t : public log_t {
    public:
        commit_log_t();
        commit_log_t(int trx_id);

        void write_log(int fd) override;
};
class rollback_log_t : public log_t {
    public:
        rollback_log_t();
        rollback_log_t(int trx_id);

        void write_log(int fd) override;
};

// Update / Compensate Log
class update_log_t : public log_t {
    public:
        uint64_t table_id;
        uint64_t page_id;
        slotnum_t offset;
        uint16_t data_length;
        std::string old_image;
        std::string new_image;

        update_log_t();
        update_log_t(int trx_id, uint64_t LSN, uint64_t prev_LSN, uint64_t table_id, uint64_t page_id, slotnum_t offset, uint16_t data_length, std::string old_image, std::string new_image);

        void write_log(int fd) override;
        void add_old_image(std::string old_img);
        void add_new_image(std::string new_img);
};
class compensate_log_t : public log_t {
    public:
        uint64_t table_id;
        uint64_t page_id;
        slotnum_t offset;
        uint16_t data_length;
        std::string old_image;
        std::string new_image;
        uint64_t next_undo_LSN;
        
        compensate_log_t();
        compensate_log_t(int trx_id, uint64_t LSN, uint64_t prev_LSN, uint64_t table_id, uint64_t page_id, slotnum_t offset, uint16_t data_length, std::string old_image, std::string new_image, uint64_t next_undo_LSN);

        void write_log(int fd) override;
        void add_old_image(std::string old_img);
        void add_new_image(std::string new_img);
};

// Log Buffer Manager
class LogBufferManager {
    private:
        std::vector<log_t*> log_buf;
        int max_size;
        char* log_path;
        char* logmsg_path;
        int log_file_fd;
        FILE* logmsg_file;

        std::set<int> win_trx;
        std::set<int> lose_trx;

        int analyze_log();
        void redo_pass();
        void undo_pass();

    public:
        uint64_t next_LSN;
        uint64_t flushed_LSN;

        LogBufferManager();
        void init(int buf_size, char* log_path, char* logmsg_path);
        void add_log(log_t* log);
        void flush_logs();
        void recovery();
};

extern LogBufferManager log_buf_manager;

#endif