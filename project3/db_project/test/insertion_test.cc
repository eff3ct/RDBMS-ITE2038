#include "db.h"

#include <gtest/gtest.h>

#include <string>
#include <random>
#include <algorithm>

// std::string get_random_string(int length) {
//     std::string ret;

//     std::mt19937 engine(time(NULL));
//     std::uniform_int_distribution<char> dis('a', 'z');
//     auto gen = bind(dis, engine);

//     for(int i = 0; i < length; ++i) {
//         ret += gen();
//     }

//     return ret;
// }

std::string get_random_string(int length) {
    std::string ret;

    std::mt19937 engine(time(NULL));
    std::uniform_int_distribution<char> dis('a', 'z');
    auto gen = std::bind(dis, engine);

    for(int i = 0; i < length; ++i) {
        ret += gen();
    }

    return ret;
}

TEST(InsertionTest, HandlesInsertionRandomRecordKey) {
    std::string pathname = "insert_randomrk_test.db";
    std::remove(pathname.c_str());

    int64_t table_id = open_table(pathname.c_str());
    EXPECT_GE(table_id, 0)
        << "open table failed.\n";

    int insertion_count = 200'000;
    std::vector<int> keys(insertion_count);
    for(int i = 0; i < insertion_count; ++i) {
        keys[i] = i;
    }
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(keys.begin(), keys.end(), g);

    std::vector<std::string> records;
    uint16_t record_size = 100;
    for(int i = 0; i < insertion_count; ++i) {
        std::string tmp = get_random_string(record_size);
        const char* record = tmp.c_str();
        records.push_back(tmp);

        int64_t key = keys[i];
        int flag = db_insert(table_id, key, record, record_size);
        EXPECT_EQ(flag, 0)
            << "insertion failed(" << i << "th insertion, " << key << " key).\n";
    }

    for(int i = 0; i < insertion_count; ++i) {
        int64_t key = keys[i];
        char ret_val[200];
        uint16_t val_size;
        int flag = db_find(table_id, key, ret_val, &val_size);
        EXPECT_EQ(flag, 0)
            << "find failed(" << key << ").\n";
        EXPECT_EQ(val_size, record_size)
            << "not expected value size( get : " << val_size << ", original : " << record_size << " ).\n";
        EXPECT_EQ(std::string(ret_val, val_size), std::string(records[i].c_str(), record_size))
            << "not expected value( get : " << ret_val << ", original : " << records[i] << " ).\n";
    }

    shutdown_db();
    std::remove(pathname.c_str());
}

TEST(InsertionTest, HandlesInsertionIncreasingOrderKey) {
    std::string pathname = "insert_inc_test.db";
    std::remove(pathname.c_str());

    int64_t table_id = open_table(pathname.c_str());
    EXPECT_GE(table_id, 0)
        << "open table failed.\n";
    
    const char* record = "<INSERTION_TEST::RECORD>|-|<INSERTION_TEST::RECORD>";
    uint16_t record_size = strlen(record);

    int insertion_count = 10'000;
    for(int i = 0; i < insertion_count; ++i) {
        int64_t key = i;
        int flag = db_insert(table_id, key, record, record_size);
        EXPECT_EQ(flag, 0)
            << "insertion failed(" << i << "th insertion, " << key << " key).\n";
    }

    for(int i = 0; i < insertion_count; ++i) {
        int64_t key = i;
        char ret_val[200];
        uint16_t val_size;
        int flag = db_find(table_id, key, ret_val, &val_size);
        EXPECT_EQ(flag, 0)
            << "find failed(" << key << ").\n";
        EXPECT_EQ(val_size, record_size)
            << "not expected value size( get : " << val_size << ", original : " << record_size << " ).\n";
        EXPECT_EQ(std::string(ret_val, val_size), std::string(record, record_size))
            << "not expected value( get : " << ret_val << ", original : " << record << " ).\n";
    }

    shutdown_db();
    std::remove(pathname.c_str());
}

TEST(InsertionTest, HandlesInsertionDecreasingOrderKey) {
    std::string pathname = "insert_dec_test.db";
    std::remove(pathname.c_str());

    int64_t table_id = open_table(pathname.c_str());
    EXPECT_GE(table_id, 0)
        << "open table failed.\n";
    
    const char* record = "<INSERTION_TEST::RECORD>|-|<INSERTION_TEST::RECORD>|-|<INSERTION_TEST::RECORD>";
    uint16_t record_size = strlen(record);

    int insertion_count = 10'000;
    for(int i = insertion_count; i > 0; --i) {
        int64_t key = i;
        int flag = db_insert(table_id, key, record, record_size);
        EXPECT_EQ(flag, 0)
            << "insertion failed(" << i << "th insertion, " << key << " key).\n";
    }

    for(int i = 1; i <= insertion_count; ++i) {
        int64_t key = i;
        char ret_val[200];
        uint16_t val_size;
        int flag = db_find(table_id, key, ret_val, &val_size);
        EXPECT_EQ(flag, 0)
            << "find failed(" << key << ").\n";
        EXPECT_EQ(val_size, record_size)
            << "not expected value size( get : " << val_size << ", original : " << record_size << " ).\n";
        EXPECT_EQ(std::string(ret_val, val_size), std::string(record, record_size))
            << "not expected value( get : " << ret_val << ", original : " << record << " ).\n";
    }

    shutdown_db();
    std::remove(pathname.c_str());
}

TEST(InsertionTest, HandlesInsertionRandomKey) {
    std::string pathname = "insert_random_test.db";
    std::remove(pathname.c_str());

    int64_t table_id = open_table(pathname.c_str());
    EXPECT_GE(table_id, 0)
        << "open table failed.\n";
    
    const char* record = "<INSERTION_TEST::RECORD>|-|<INSERTION_TEST::RECORD>|-|<INSERTION_TEST::RECORD>";
    uint16_t record_size = strlen(record);

    int insertion_count = 10'000;
    std::vector<int> keys(insertion_count);
    for(int i = 0; i < insertion_count; ++i) {
        keys[i] = i;
    }
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(keys.begin(), keys.end(), g);

    for(int i = insertion_count; i > 0; --i) {
        int64_t key = keys[i];
        int flag = db_insert(table_id, key, record, record_size);
        EXPECT_EQ(flag, 0)
            << "insertion failed(" << i << "th insertion, " << key << " key).\n";
    }

    for(int i = 1; i <= insertion_count; ++i) {
        int64_t key = keys[i];
        char ret_val[200];
        uint16_t val_size;
        int flag = db_find(table_id, key, ret_val, &val_size);
        EXPECT_EQ(flag, 0)
            << "find failed(" << key << ").\n";
        EXPECT_EQ(val_size, record_size)
            << "not expected value size( get : " << val_size << ", original : " << record_size << " ).\n";
        EXPECT_EQ(std::string(ret_val, val_size), std::string(record, record_size))
            << "not expected value( get : " << ret_val << ", original : " << record << " ).\n";
    }

    shutdown_db();
    std::remove(pathname.c_str());
}