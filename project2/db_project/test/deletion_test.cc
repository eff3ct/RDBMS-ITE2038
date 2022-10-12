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

std::string get_random_stringt(int length) {
    std::string ret;

    std::mt19937 engine(time(NULL));
    std::uniform_int_distribution<char> dis('a', 'z');
    auto gen = std::bind(dis, engine);

    for(int i = 0; i < length; ++i) {
        ret += gen();
    }

    return ret;
}

TEST(DeletionTest, HandlesDeletion) {
    std::string pathname = "delete_test.db";
    std::remove(pathname.c_str());

    int64_t table_id = open_table(pathname.c_str());
    EXPECT_GE(table_id, 0)
        << "open table failed.\n";

    const char* record = "<INSERTION_TEST::RECORD>|-|<INSERTION_TEST::RECORD>";
    uint16_t record_size = strlen(record);

    int insertion_count = 10000;
    for(int i = 0; i < insertion_count; ++i) {
        int64_t key = i;
        int flag = db_insert(table_id, key, record, record_size);
        EXPECT_EQ(flag, 0)
            << "insertion failed(" << i << "th insertion, " << key << " key).\n";
    }

    for(int i = 0; i < insertion_count; ++i) {
        int64_t key = i;
        int flag = db_delete(table_id, key);
        EXPECT_EQ(flag, 0)
            << "deletion failed(" << i << "th deletion, " << key << " key).\n";
        
        char ret_val[200];
        uint16_t val_size;
        int flag2 = db_find(table_id, key, ret_val, &val_size);
        EXPECT_EQ(flag2, -1)
            << "find failed(deleted key found)::(" << key << ").\n";
    }
}

TEST(DeletionTest, HandlesDeletionRandomRecordKey) {
    std::string pathname = "delete_randomrk_test.db";
    std::remove(pathname.c_str());

    int64_t table_id = open_table(pathname.c_str());
    EXPECT_GE(table_id, 0)
        << "open table failed.\n";

    int insertion_count = 300000;
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
        std::string tmp = get_random_stringt(record_size);
        const char* record = tmp.c_str();
        records.push_back(tmp);

        int64_t key = keys[i];
        int flag = db_insert(table_id, key, record, record_size);
        EXPECT_EQ(flag, 0)
            << "insertion failed(" << i << "th insertion, " << key << " key).\n";
    }

    for(int i = 0; i < insertion_count; ++i) {
        int64_t key = keys[i];
        int flag_d = db_delete(table_id, key);
        EXPECT_EQ(flag_d, 0)
            << "deletion failed(" << i << "th deletion, " << key << " key).\n";
        char ret_val[200];
        uint16_t val_size;
        int flag = db_find(table_id, key, ret_val, &val_size);
        EXPECT_EQ(flag, -1)
            << "find failed :: (dup found) (" << key << ").\n";
    }

    shutdown_db();
    std::remove(pathname.c_str());
}