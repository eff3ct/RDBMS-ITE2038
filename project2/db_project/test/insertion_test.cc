#include "db.h"

#include <gtest/gtest.h>

#include <string>

TEST(InsertionTest, HandlesInsertion) {
    int64_t table_id;
    std::string pathname = "insertion_test.db";
    std::remove(pathname.c_str());

    // Open a database file
    table_id = open_table(pathname.c_str());

    // Check if the file is opened
    ASSERT_TRUE(table_id >= 0);

    const char* value = "value9918293164231";
    uint16_t size = strlen(value);
    int rec = 5000000;
    // Insert 100 records
    for(int i = 0; i < rec; ++i) {
        int flag = db_insert(table_id, i, value, size);
        EXPECT_EQ(flag, 0);
    }

    // Check if the records are inserted
    for(int i = 0; i < rec; ++i) {
        char ret_val[120];
        uint16_t val_size;
        int flag = db_find(table_id, i, ret_val, &val_size);
        EXPECT_EQ(flag, 0)
            << "key: " << i << " value: " << ret_val << " size: " << val_size;
        EXPECT_EQ(val_size, size);
        EXPECT_EQ(std::string(ret_val, val_size), value)
            << "i: " << i << ", ret_val: " << ret_val << ", val_size: " << val_size << '\n';
    }

    shutdown_db();
}

// TEST(FindTest, HandlesFind) {
//     int64_t table_id;
//     std::string pathname = "find_test.db";
//     std::remove(pathname.c_str());

//     // Open a database file
//     table_id = open_table(pathname.c_str());

//     // Check if the file is opened
//     ASSERT_TRUE(table_id >= 0);

//     const char* value = "valueasdf12nnnfd9hhajibvidns01237dabasijgas";
//     uint16_t size = 44;
//     // Insert 1 record
//     int flag = db_insert(table_id, 154, value, size);

//     // Check if the record is inserted
//     EXPECT_EQ(flag, 0);
    
//     char* ret_val = new char[100];
//     uint16_t val_size;
//     flag = db_find(table_id, 154, ret_val, &val_size);
//     EXPECT_EQ(flag, 0);

//     // Check if the record is found
//     EXPECT_EQ(std::string(ret_val, val_size), std::string(value, size))
//         << "The value is not the same as the inserted one\n";

//     delete[] ret_val;

//     shutdown_db();
// }