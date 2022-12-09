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

using slotnum_t = uint16_t;

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

        void write_log(int fd) override;
};
class commit_log_t : public log_t {
    public:
        commit_log_t();

        void write_log(int fd) override;
};
class rollback_log_t : public log_t {
    public:
        rollback_log_t();

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

        void write_log(int fd) override;
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

        void write_log(int fd) override;
};

// Log Buffer Manager
class LogBufferManager {
    private:
        std::vector<log_t*> log_buf;
        int max_size;
        int log_fd;
        // int log_msg_fd;

    public:
        LogBufferManager();
        void init_buf(int buf_size);
        void add_log(log_t* log);
        void flush_logs();
};

extern LogBufferManager log_buf_manager;

#endif