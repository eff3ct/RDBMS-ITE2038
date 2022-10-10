#include "page.h"

/* TODO */
// 1. optimize read page using overloaded function.

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
uint16_t page_io::get_key_count(const page_t* page) {
    uint16_t key_count;
    memcpy(&key_count, page->data + sizeof(pagenum_t) + sizeof(uint16_t), sizeof(uint16_t));
    return key_count;
}
// Set key count of page
void page_io::set_key_count(page_t* page, uint16_t key_count) {
    memcpy(page->data + sizeof(pagenum_t) + sizeof(uint16_t), &key_count, sizeof(uint16_t));
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

/* LEAF PAGE IO */
// Set a new leaf page
void page_io::leaf::set_new_leaf_page(page_t* leaf_page, pagenum_t parent_pagenum) {
    int16_t is_leaf = 1;
    uint16_t key_count = 0;
    memcpy(leaf_page->data, &parent_pagenum, sizeof(pagenum_t));
    memcpy(leaf_page->data + sizeof(pagenum_t), &is_leaf, sizeof(int16_t));
    memcpy(leaf_page->data + sizeof(pagenum_t) + sizeof(uint16_t), &key_count, sizeof(uint16_t));
    
    pagenum_t free_space = PAGE_SIZE - 128;
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
    memcpy(leaf_page->data + LEAF_PAGE_OFFSET + 2 * sizeof(pagenum_t) + slot_num * sizeof(slot_t), slot, sizeof(slot_t));
}
// Set record to leaf page.
void page_io::leaf::set_record(page_t* leaf_page, slotnum_t offset, const char* value, int16_t size) {
    memcpy(leaf_page->data + offset, value, size);
}
// Get amount of free space left.
pagenum_t page_io::leaf::get_free_space(const page_t* leaf_page) {
    pagenum_t amount_of_free_space;
    memcpy(&amount_of_free_space, leaf_page->data + LEAF_PAGE_OFFSET, sizeof(pagenum_t));
    return amount_of_free_space;
}
// Get right sibling page number.
pagenum_t page_io::leaf::get_right_sibling(const page_t* leaf_page) {
    pagenum_t right_sibling_page;
    memcpy(&right_sibling_page, leaf_page->data + LEAF_PAGE_OFFSET + sizeof(pagenum_t), sizeof(pagenum_t));
    return right_sibling_page;
}
// Get key from (slot_num) slot.
pagenum_t page_io::leaf::get_key(const page_t* leaf_page, slotnum_t slot_num) {
    pagenum_t key;
    memcpy(&key, leaf_page->data + LEAF_PAGE_OFFSET + SLOT_SIZE * slot_num, sizeof(pagenum_t));
    return key;
}
slotnum_t page_io::leaf::get_offset(const page_t* leaf_page, slotnum_t slot_num) {
    slotnum_t offset;
    memcpy(&offset, leaf_page->data + LEAF_PAGE_OFFSET + SLOT_SIZE * slot_num + sizeof(pagenum_t) + sizeof(slotnum_t), sizeof(slotnum_t));
    return offset;
}

/* SLOT IO */
// Get slot from (slot_num) slot.
void slot_io::read_slot(const page_t* page, slotnum_t slot_num, slot_t* slot) {
    memcpy(slot, page->data + LEAF_PAGE_OFFSET + SLOT_SIZE * slot_num, SLOT_SIZE);
}


