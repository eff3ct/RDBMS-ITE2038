#ifndef LOG_H
#define LOG_H

#define BEGIN 0
#define UPDATE 1
#define COMMIT 2
#define ROLLBACK 3
#define COMPENSATE 4

#include <string.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>

#include <string>
#include <vector>

using slotnum_t = uint16_t;

// Log Buffer Manager Latch
extern pthread_mutex_t log_buf_manager_latch;

// Base Log Structure
class log_t {
    public:
        uint32_t log_size;
        uint64_t LSN;
        uint64_t prev_LSN;
        uint32_t trx_id;
        uint32_t log_type;

        log_t();

        void write_log(int fd);
};

// Begin / Commit / Rollback Log
class begin_log_t : public log_t {
    public:
        begin_log_t();

        // override
        void write_log(int fd);
};
class commit_log_t : public log_t {
    public:
        commit_log_t();

        // override
        void write_log(int fd);
};
class rollback_log_t : public log_t {
    public:
        rollback_log_t();

        // override
        void write_log(int fd);
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

        // override
        void write_log(int fd);
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

        // override
        void write_log(int fd);
};

// Log Buffer Manager
class LogBufferManager {
    private:
        std::vector<log_t*> log_buf;

    public:
        void init_buf();
        void add_log(int fd, log_t* log);
        void flush_logs();
};

extern LogBufferManager log_buf_manager;

#endif