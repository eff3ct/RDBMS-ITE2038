#ifndef PAGE_H
#define PAGE_H

#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>

#define INITIAL_DB_FILE_SIZE (10 * 1024 * 1024)  // 10 MiB
#define PAGE_SIZE (4 * 1024)                     // 4 KiB
#define MAGIC_NUMBER (2022)                      // MAGIC NUMBER for DB file

#define LEAF_PAGE_OFFSET (112)
#define INTERNAL_PAGE_OFFSET (120)
#define SLOT_SIZE (12)
#define INTERNAL_ORDER (124)

typedef uint64_t pagenum_t;
typedef uint8_t slotnum_t;

struct page_t {
    char data[PAGE_SIZE];
};

struct slot_t {
    char data[SLOT_SIZE];
};

namespace page_io {
    pagenum_t get_next_free_page(const page_t* page, pagenum_t pagenum);
    bool is_leaf(const page_t* page);
    uint16_t get_key_count(const page_t* page);
    void set_key_count(page_t* page, uint16_t key_count);
    pagenum_t get_parent_page(const page_t* page);

    namespace header {
        void set_header_page(page_t* header_page, pagenum_t next_free_page, pagenum_t page_cnt, pagenum_t root_page);
        pagenum_t get_root_page(const page_t* header_page);
        pagenum_t get_page_count(const page_t* header_page);
    }
    namespace internal {
    
    }
    namespace leaf {
        void set_new_leaf_page(page_t* leaf_page);
        void update_free_space(page_t* leaf_page, slotnum_t size);
        void set_slot(page_t* leaf_page, slotnum_t slot_num, const slot_t* slot);
        void set_record(page_t* leaf_page, slotnum_t offset, const char* value, int16_t size);
        pagenum_t get_free_space(const page_t* leaf_page);
        pagenum_t get_right_sibling(const page_t* leaf_page);
        pagenum_t get_key(const page_t* leaf_page, slotnum_t slot_num);
        slotnum_t get_offset(const page_t* leaf_page, slotnum_t slot_num);
    }
}

namespace slot_io {
    void read_slot(const page_t* page, slotnum_t slot_num, slot_t* slot);
    void set_new_slot(slot_t* slot, int64_t key, slotnum_t size, slotnum_t offset);
    slotnum_t get_record_size(const slot_t* slot);
}

#endif