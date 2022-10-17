#include "page.h"

/* PAGE IO */
// Get next free page number from page(pagenum)
pagenum_t page_io::get_next_free_page(const page_t* page, pagenum_t pagenum) {
    pagenum_t next_free_page;
    int idx = (pagenum == 0);
    memcpy(&next_free_page, page->data + idx * sizeof(pagenum_t), sizeof(pagenum_t));
    return next_free_page;
}
// Get whether page is leaf
bool page_io::is_leaf(const page_t* page) {
    int16_t is_leaf_val;
    memcpy(&is_leaf_val, page->data + sizeof(pagenum_t), sizeof(int16_t));
    bool is_leaf = (is_leaf_val == 1);
    return is_leaf;
}
// Get number of keys of page
uint32_t page_io::get_key_count(const page_t* page) {
    uint32_t key_count;
    memcpy(&key_count, page->data + sizeof(pagenum_t) + sizeof(uint32_t), sizeof(uint32_t));
    return key_count;
}
// Set key count of page
void page_io::set_key_count(page_t* page, uint32_t key_count) {
    memcpy(page->data + KEY_COUNT_OFFSET, &key_count, sizeof(uint32_t));
}
pagenum_t page_io::get_parent_page(const page_t* page) {
    pagenum_t parent_page;
    memcpy(&parent_page, page->data, sizeof(pagenum_t));
    return parent_page;
}
void page_io::set_parent_page(page_t* page, pagenum_t parent_page) {
    memcpy(page->data, &parent_page, sizeof(pagenum_t));
}

/* HEADER PAGE IO */

/* Set header page of file
* [0] = magic number(2022), 
* [1] = next page number(next_free_page), 
* [2] = number of pages(page_cnt),
* [3] = root page number
*/
void page_io::header::set_header_page(page_t* header_page, pagenum_t next_free_page, pagenum_t page_cnt, pagenum_t root_page) {
    pagenum_t magic_number = MAGIC_NUMBER;

    memcpy(header_page->data, &magic_number, sizeof(pagenum_t));
    memcpy(header_page->data + sizeof(pagenum_t), &next_free_page, sizeof(pagenum_t));
    memcpy(header_page->data + sizeof(pagenum_t) * 2, &page_cnt, sizeof(pagenum_t));
    memcpy(header_page->data + sizeof(pagenum_t) * 3, &root_page, sizeof(pagenum_t));
}
void page_io::header::set_root_page(page_t* header_page, pagenum_t root_page) {
    memcpy(header_page->data + sizeof(pagenum_t) * 3, &root_page, sizeof(pagenum_t));
}
// Get root page number.
pagenum_t page_io::header::get_root_page(const page_t* header_page) {
    pagenum_t root_page;
    memcpy(&root_page, header_page->data + sizeof(pagenum_t) * 3, sizeof(pagenum_t));
    return root_page;
}
// Get total page count from header page.
pagenum_t page_io::header::get_page_count(const page_t* header_page) {
    pagenum_t page_cnt;
    memcpy(&page_cnt, header_page->data + sizeof(pagenum_t) * 2, sizeof(pagenum_t));
    return page_cnt;
}

/* INTERNAL PAGE IO */
void page_io::internal::set_new_internal_page(page_t* internal_page) {
    pagenum_t parent_page = 0;
    int32_t is_leaf = 0;
    uint32_t key_count = 0;

    memcpy(internal_page->data, &parent_page, sizeof(pagenum_t));
    memcpy(internal_page->data + sizeof(pagenum_t), &is_leaf, sizeof(int32_t));
    memcpy(internal_page->data + sizeof(pagenum_t) + sizeof(int32_t), &key_count, sizeof(uint32_t));
}

int64_t page_io::internal::get_key(const page_t* internal_page, pagenum_t idx) {
    int64_t key;
    memcpy(&key, internal_page->data + INTERNAL_PAGE_OFFSET + sizeof(pagenum_t) + idx * 2 * sizeof(pagenum_t), sizeof(int64_t));
    return key;
}
pagenum_t page_io::internal::get_child(const page_t* internal_page, pagenum_t idx) {
    pagenum_t child;
    memcpy(&child, internal_page->data + INTERNAL_PAGE_OFFSET + idx * 2 * sizeof(pagenum_t), sizeof(pagenum_t));
    return child;
}
void page_io::internal::set_key(page_t* internal_page, pagenum_t idx, int64_t key) {
    memcpy(internal_page->data + INTERNAL_PAGE_OFFSET + sizeof(pagenum_t) + idx * 2 * sizeof(pagenum_t), &key, sizeof(int64_t));
}
void page_io::internal::set_child(page_t* internal_page, pagenum_t idx, pagenum_t child) {
    memcpy(internal_page->data + INTERNAL_PAGE_OFFSET + idx * 2 * sizeof(pagenum_t), &child, sizeof(pagenum_t));
}

/* LEAF PAGE IO */
// Set a new leaf page
void page_io::leaf::set_new_leaf_page(page_t* leaf_page) {
    int32_t is_leaf = 1;
    uint32_t key_count = 0;
    pagenum_t parent_pagenum = 0;
    memcpy(leaf_page->data, &parent_pagenum, sizeof(pagenum_t));
    memcpy(leaf_page->data + sizeof(pagenum_t), &is_leaf, sizeof(int32_t));
    memcpy(leaf_page->data + sizeof(pagenum_t) + sizeof(uint32_t), &key_count, sizeof(uint32_t));
    
    pagenum_t free_space = INITIAL_FREE_SPACE;
    pagenum_t right_sibling = 0;
    memcpy(leaf_page->data + LEAF_PAGE_OFFSET, &free_space, sizeof(pagenum_t));
    memcpy(leaf_page->data + LEAF_PAGE_OFFSET + sizeof(pagenum_t), &right_sibling, sizeof(pagenum_t));
}
// Modify free space of leaf page.
void page_io::leaf::update_free_space(page_t* leaf_page, slotnum_t size) {
    pagenum_t free_space = page_io::leaf::get_free_space(leaf_page);
    free_space -= size;
    memcpy(leaf_page->data + LEAF_PAGE_OFFSET, &free_space, sizeof(pagenum_t));
}
// Set slot to leaf page.
void page_io::leaf::set_slot(page_t* leaf_page, slotnum_t slot_num, const slot_t* slot) {
    memcpy(leaf_page->data + LEAF_PAGE_SLOT_OFFSET + slot_num * SLOT_SIZE, slot->data, SLOT_SIZE);
}
// Set record to leaf page.
void page_io::leaf::set_record(page_t* leaf_page, slotnum_t offset, const char* value, uint16_t size) {
    memcpy(leaf_page->data + offset, value, size);
}
// Get amount of free space left.
pagenum_t page_io::leaf::get_free_space(const page_t* leaf_page) {
    pagenum_t amount_of_free_space;
    memcpy(&amount_of_free_space, leaf_page->data + LEAF_PAGE_OFFSET, sizeof(pagenum_t));
    return amount_of_free_space;
}
void page_io::leaf::set_free_space(page_t* leaf_page, pagenum_t free_space) {
    memcpy(leaf_page->data + LEAF_PAGE_OFFSET, &free_space, sizeof(pagenum_t));
}
void page_io::leaf::set_right_sibling(page_t* leaf_page, pagenum_t right_sibling) {
    memcpy(leaf_page->data + LEAF_PAGE_OFFSET + sizeof(pagenum_t), &right_sibling, sizeof(pagenum_t));
}
// Get right sibling page number.
pagenum_t page_io::leaf::get_right_sibling(const page_t* leaf_page) {
    pagenum_t right_sibling_page;
    memcpy(&right_sibling_page, leaf_page->data + LEAF_PAGE_OFFSET + sizeof(pagenum_t), sizeof(pagenum_t));
    return right_sibling_page;
}
// Get key from (slot_num) slot.
int64_t page_io::leaf::get_key(const page_t* leaf_page, slotnum_t slot_num) {
    int64_t key;
    memcpy(&key, leaf_page->data + LEAF_PAGE_SLOT_OFFSET + SLOT_SIZE * slot_num, sizeof(pagenum_t));
    return key;
}
void page_io::leaf::get_record(const page_t* leaf_page, slotnum_t offset, char* value, uint16_t size) {
    memcpy(value, leaf_page->data + offset, size);
}
slotnum_t page_io::leaf::get_record_size(const page_t* leaf_page, slotnum_t slot_num) {
    slotnum_t record_size;
    memcpy(&record_size, leaf_page->data + LEAF_PAGE_SLOT_OFFSET + SLOT_SIZE * slot_num + sizeof(pagenum_t), sizeof(slotnum_t));
    return record_size;
}
slotnum_t page_io::leaf::get_offset(const page_t* leaf_page, slotnum_t slot_num) {
    slotnum_t offset;
    memcpy(&offset, leaf_page->data + LEAF_PAGE_SLOT_OFFSET + SLOT_SIZE * slot_num + sizeof(pagenum_t) + sizeof(slotnum_t), sizeof(slotnum_t));
    return offset;
}

/* SLOT IO */
// Get slot from (slot_num) slot.
void slot_io::read_slot(const page_t* page, slotnum_t slot_num, slot_t* slot) {
    memcpy(slot->data, page->data + LEAF_PAGE_SLOT_OFFSET + SLOT_SIZE * slot_num, SLOT_SIZE);
}
// Set a new slot with data.
void slot_io::set_new_slot(slot_t* slot, int64_t key, slotnum_t size, slotnum_t offset) {
    memcpy(slot->data, &key, sizeof(int64_t));
    memcpy(slot->data + sizeof(int64_t), &size, sizeof(slotnum_t));
    memcpy(slot->data + sizeof(int64_t) + sizeof(slotnum_t), &offset, sizeof(slotnum_t));
}
void slot_io::set_offset(slot_t* slot, slotnum_t offset) {
    memcpy(slot->data + sizeof(int64_t) + sizeof(slotnum_t), &offset, sizeof(slotnum_t));
}
slotnum_t slot_io::get_offset(const slot_t* slot) {
    slotnum_t offset;
    memcpy(&offset, slot->data + sizeof(int64_t) + sizeof(slotnum_t), sizeof(slotnum_t));
    return offset;
}
slotnum_t slot_io::get_record_size(const slot_t* slot) {
    slotnum_t size;
    memcpy(&size, slot->data + sizeof(int64_t), sizeof(slotnum_t));
    return size;
}
int64_t slot_io::get_key(const slot_t* slot) {
    int64_t key;
    memcpy(&key, slot->data, sizeof(int64_t));
    return key;
}