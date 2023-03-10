#include "file.h"

#include <gtest/gtest.h>

#include <string>

/*******************************************************************************
 * The test structures stated here were written to give you and idea of what a
 * test should contain and look like. Feel free to change the code and add new
 * tests of your own. The more concrete your tests are, the easier it'd be to
 * detect bugs in the future projects.
 ******************************************************************************/

/*
* Tests file open/close APIs.
* 1. Open a file and check the descriptor
* 2. Check if the file's initial size is 10 MiB
*/
TEST(FileInitTest, HandlesInitialization) {
    int64_t table_id;                                 // file descriptor
    std::string pathname = "init_test.db";  // customize it to your test file
    remove(pathname.c_str());

    // Open a database file
    table_id = file_open_table_file(pathname.c_str());

    // Check if the file is opened
    ASSERT_TRUE(table_id >= 0);  // change the condition to your design's behavior

    // Check the size of the initial file
    page_t header_page;
    file_read_page(table_id, 0, &header_page);

    pagenum_t cnt;
    memcpy(&cnt, header_page.data + sizeof(pagenum_t) * 2, sizeof(pagenum_t));
    int num_pages = cnt;
    EXPECT_EQ(num_pages, INITIAL_DB_FILE_SIZE / PAGE_SIZE)
        << "The initial number of pages does not match the requirement: "
        << num_pages;

    // off_t file_size = get_file_size(table_id);
    // EXPECT_EQ(file_size, INITIAL_DB_FILE_SIZE)
    //     << "The initial file size does not match the requirement: " 
    //     << file_size;

    // Close all database files
    file_close_table_files();

    // Remove the db file
    remove(pathname.c_str());
}

/*
 * Tests page allocation and free
 * 1. Allocate 2 pages and free one of them, traverse the free page list
 *    and check the existence/absence of the freed/allocated page
 */
TEST(PageTest, HandlesPageAllocation) {
    std::string pathname = "page_test.db";
    remove(pathname.c_str());

    int64_t table_id = file_open_table_file(pathname.c_str());

    pagenum_t allocated_page, freed_page;

    // Allocate the pages
    allocated_page = file_alloc_page(table_id);
    freed_page = file_alloc_page(table_id);

    // Free one page
    file_free_page(table_id, freed_page);

    // Traverse the free page list and check the existence of the freed/allocated
    // pages. You might need to open a few APIs soley for testing.
    bool is_freed_page_exist = false;

    page_t cur_page;
    pagenum_t next_free_page;
    file_read_page(table_id, 0, &cur_page);
    memcpy(&next_free_page, cur_page.data + sizeof(pagenum_t), sizeof(pagenum_t));
    while(next_free_page != 0) {
        if(next_free_page == freed_page) is_freed_page_exist = true;
     
        file_read_page(table_id, next_free_page, &cur_page);
        memcpy(&next_free_page, cur_page.data, sizeof(pagenum_t));
    }

    file_close_table_files();

    remove(pathname.c_str());

    EXPECT_TRUE(is_freed_page_exist)
        << "The freed page does not exist in the free page list";
}

/*
 * Tests page read/write operations
 * 1. Write/Read a page with some random content and check if the data matches
 */ 
TEST(PageIOTest, CheckReadWriteOperation) {
    std::string pathname = "page_io_test.db";
    remove(pathname.c_str());

    int64_t table_id = file_open_table_file(pathname.c_str());

    pagenum_t allocated_page = file_alloc_page(table_id);

    page_t a_page;
    memset(a_page.data, 'a', PAGE_SIZE);
    file_write_page(table_id, allocated_page, &a_page);

    page_t check_page;
    file_read_page(table_id, allocated_page, &check_page);
    EXPECT_EQ(memcmp(a_page.data, check_page.data, PAGE_SIZE), 0)
        << "The written data does not match the read data";

    file_close_table_files();

    remove(pathname.c_str());
}

/*
 * Advanced test: Page Write Test
 * 1. Test page write by writing a page with a random content
 */
TEST(PageIOTest, AdvancedWritingTest) {
    std::string pathname = "page_io_test.db";
    remove(pathname.c_str());

    int64_t table_id = file_open_table_file(pathname.c_str());

    pagenum_t allocated_page = file_alloc_page(table_id);

    char random_data[PAGE_SIZE] = "Hello, World! asdfadsfbewbfhiebvifdnvksdfvk sd v ksadbjashdb";
    page_t a_page;
    memcpy(a_page.data, random_data, PAGE_SIZE);
    file_write_page(table_id, allocated_page, &a_page);

    page_t check_page;
    file_read_page(table_id, allocated_page, &check_page);
    EXPECT_EQ(memcmp(a_page.data, check_page.data, PAGE_SIZE), 0)
        << "The written data does not match the read data";

    file_close_table_files();

    remove(pathname.c_str());
}

/* 
 * Tests file doubling
 * 1. Allocate pages until the file size doubles
 * 2. Check if the file size is doubled
 */
TEST(FileDoublingTest, CheckFileDoubling) {
    std::string pathname = "file_doubling_test.db";
    remove(pathname.c_str());

    int64_t table_id = file_open_table_file(pathname.c_str());

    for(int i = 1; i <= 2560; ++i) file_alloc_page(table_id);
    file_alloc_page(table_id);

    int fd = table_manager.get_fd(table_id);
    off_t file_size = file_io::get_file_size(fd);
    EXPECT_EQ(file_size, INITIAL_DB_FILE_SIZE * 2)
        << "The file size does not match the requirement: " << file_size;

    file_close_table_files();

    remove(pathname.c_str());
}
