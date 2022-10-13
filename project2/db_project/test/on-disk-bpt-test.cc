#include "db.h"

#include <gtest/gtest.h>

#include <string>
#include <random>
#include <algorithm>

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

TEST(OnDiskBplusTreeTest, DoubleOpenTest) {
    int64_t table1 = open_table("test.db");
    int64_t table2 = open_table("test.db");
    EXPECT_EQ(table1, 0)
        << "FAIL : open table failed.\n";
    EXPECT_EQ(table2, -1)
        << "FAIL : double opened.\n";
    shutdown_db();
}

TEST(OnDiskBplusTreeTest, NormalInsertionTest) {
    if(std::remove("normal_insertion_test.db")) std::cout << "ALERT : remove existing file.\n";
    int64_t table_id = open_table("normal_insertion_test.db");
    EXPECT_GE(table_id, 0)
        << "open table failed.\n";
    
    uint16_t record_size = 100;
    const char* record = get_random_string(record_size).c_str();

    for(int i = 0; i < 100; ++i) {
        int64_t key = i;
        int flag = db_insert(table_id, key, record, record_size);
        EXPECT_EQ(flag, 0)
            << "insertion failed(" << i << "th insertion, " << key << " key).\n";
    }
    
    for(int i = 0; i < 100; ++i) {
        int64_t key = i;
        char* ret_record = (char*)malloc(sizeof(char) * record_size);
        int flag = db_find(table_id, key, ret_record, &record_size);
        EXPECT_EQ(flag, 0)
            << "find failed(" << i << "th find, " << key << " key).\n";
        EXPECT_EQ(std::string(record), std::string(ret_record))
            << "record is not same(" << i << "th find, " << key << " key).\n";
        free(ret_record);
    }

    EXPECT_EQ(shutdown_db(), 0)
        << "shutdown_db failed.\n";
}

TEST(OnDiskBplusTreeTest, NormalDeletionTest) {
    int64_t table_id = open_table("normal_insertion_test.db");
    EXPECT_GE(table_id, 0)
        << "open table failed.\n";
    
    uint16_t record_size = 100;
    char buf[200];    

    for(int i = 0; i < 100; ++i) {
        int64_t key = i;
        EXPECT_EQ(db_find(table_id, key, buf, &record_size), 0)
            << "FAIL : find failed / key has not been found. (" << i << "th find, " << key << " key).\n";
        int flag = db_delete(table_id, key);
        EXPECT_EQ(flag, 0)
            << "deletion failed(" << i << "th deletion, " << key << " key).\n";

        EXPECT_EQ(db_find(table_id, key, buf, &record_size), -1)
            << "find failed / key has been found. (" << i << "th find, " << key << " key).\n";
    }

    EXPECT_EQ(shutdown_db(), 0)
        << "shutdown_db failed.\n";
}

TEST(OnDiskBplusTreeTest, LargeInsertionTest) {
    if(std::remove("large_insertion_test.db")) std::cout << "ALERT : remove existing file.\n";
    int64_t table_id = open_table("large_insertion_test.db");
    EXPECT_GE(table_id, 0)
        << "open table failed.\n";
    
    uint16_t record_size = 100;
    const char* record = get_random_string(record_size).c_str();

    for(int i = 0; i < 10'000; ++i) {
        int64_t key = i;
        int flag = db_insert(table_id, key, record, record_size);
        EXPECT_EQ(flag, 0)
            << "insertion failed(" << i << "th insertion, " << key << " key).\n";
    }
    
    for(int i = 0; i < 10'000; ++i) {
        int64_t key = i;
        char* ret_record = (char*)malloc(sizeof(char) * record_size);
        int flag = db_find(table_id, key, ret_record, &record_size);
        EXPECT_EQ(flag, 0)
            << "find failed(" << i << "th find, " << key << " key).\n";
        EXPECT_EQ(std::string(record), std::string(ret_record))
            << "record is not same(" << i << "th find, " << key << " key).\n";
        free(ret_record);
    }

    EXPECT_EQ(shutdown_db(), 0)
        << "shutdown_db failed.\n";
}

TEST(OnDiskBplusTreeTest, LargeDeletionTest) {
    int64_t table_id = open_table("large_insertion_test.db");
    EXPECT_GE(table_id, 0)
        << "open table failed.\n";
    
    uint16_t record_size = 100;
    char buf[200];    
    
    for(int i = 0; i < 10'000; ++i) {
        int64_t key = i;
        EXPECT_EQ(db_find(table_id, key, buf, &record_size), 0)
            << "FAIL : find failed / key has not been found. (" << i << "th find, " << key << " key).\n";
        int flag = db_delete(table_id, key);
        EXPECT_EQ(flag, 0)
            << "deletion failed(" << i << "th deletion, " << key << " key).\n";
        EXPECT_EQ(db_find(table_id, key, buf, &record_size), -1)
            << "find failed / key has been found. (" << i << "th find, " << key << " key).\n";
    }

    EXPECT_EQ(shutdown_db(), 0)
        << "shutdown_db failed.\n";
}


TEST(OnDiskBplusTreeTest, VeryLargeInsertionTest) {
    if(std::remove("very_large_insertion_test.db")) std::cout << "ALERT : remove existing file.\n";
    int64_t table_id = open_table("very_large_insertion_test.db");
    EXPECT_GE(table_id, 0)
        << "FAIL : open table failed.\n";
    
    uint16_t record_size = 100;
    const char* record = get_random_string(record_size).c_str();

    for(int i = 0; i < 100'000; ++i) {
        int64_t key = i;
        int flag = db_insert(table_id, key, record, record_size);
        EXPECT_EQ(flag, 0)
            << "FAIL : insertion failed(" << i << "th insertion, " << key << " key).\n";
    }
    
    for(int i = 0; i < 100'000; ++i) {
        int64_t key = i;
        char* ret_record = (char*)malloc(sizeof(char) * record_size);
        int flag = db_find(table_id, key, ret_record, &record_size);
        
        EXPECT_EQ(flag, 0)
            << "FAIL : find failed(" << i << "th find, " << key << " key).\n";
        EXPECT_EQ(std::string(record), std::string(ret_record))
            << "FAIL : record is not same(" << i << "th find, " << key << " key).\n";
        free(ret_record);
    }

    EXPECT_EQ(shutdown_db(), 0)
        << "FAIL : shutdown_db failed.\n";
}

TEST(OnDiskBplusTreeTest, VeryLargeDeletionTest) {
    int64_t table_id = open_table("very_large_insertion_test.db");
    EXPECT_GE(table_id, 0)
        << "FAIL : open table failed.\n";
    
    uint16_t record_size = 100;
    char buf[200];    
    
    for(int i = 0; i < 100'000; ++i) {
        int64_t key = i;
        EXPECT_EQ(db_find(table_id, key, buf, &record_size), 0)
            << "FAIL : find failed / key has not been found. (" << i << "th find, " << key << " key).\n";
        int flag = db_delete(table_id, key);
        EXPECT_EQ(flag, 0)
            << "FAIL : deletion failed(" << i << "th deletion, " << key << " key).\n";
        EXPECT_EQ(db_find(table_id, key, buf, &record_size), -1)
            << "FAIL : find failed / key has been found. (" << i << "th find, " << key << " key).\n";
    }

    EXPECT_EQ(shutdown_db(), 0)
        << "FAIL : shutdown_db failed.\n";
}

TEST(OnDiskBplusTreeTest, HandlesInsertionRandomRecordKey) {
    if(std::remove("insert_random_record_key_test.db")) std::cout << "ALERT : remove existing file.\n";
    std::string pathname = "insert_random_record_key_test.db";

    int64_t table_id = open_table(pathname.c_str());
    EXPECT_GE(table_id, 0)
        << "FAIL : open table failed.\n";

    int insertion_count = 100'000;
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
            << "FAIL : insertion failed(" << i << "th insertion, " << key << " key).\n";
    }

    for(int i = 0; i < insertion_count; ++i) {
        int64_t key = keys[i];
        char ret_val[200];
        uint16_t val_size;
        int flag = db_find(table_id, key, ret_val, &val_size);
        EXPECT_EQ(flag, 0)
            << "FAIL : find failed(" << key << ").\n";
        EXPECT_EQ(val_size, record_size)
            << "FAIL : not expected value size( get : " << val_size << ", original : " << record_size << " ).\n";
        EXPECT_EQ(std::string(ret_val, val_size), std::string(records[i].c_str(), record_size))
            << "FAIL : not expected value( get : " << ret_val << ", original : " << records[i] << " ).\n";
    }

    EXPECT_EQ(shutdown_db(), 0)
        << "FAIL : shutdown_db failed.\n";
}

TEST(OnDiskBplusTreeTest, HandlesDeletionRandomRecordKey) {
    std::string pathname = "insert_random_record_key_test.db";

    int64_t table_id = open_table(pathname.c_str());
    EXPECT_GE(table_id, 0)
        << "FAIL : open table failed.\n";

    int insertion_count = 100'000;
    std::vector<int> keys(insertion_count);
    for(int i = 0; i < insertion_count; ++i) {
        keys[i] = i;
    }
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(keys.begin(), keys.end(), g);

    for(int i = 0; i < insertion_count; ++i) {
        int64_t key = keys[i];
        char buf[200];
        uint16_t val_size;
        EXPECT_EQ(db_find(table_id, key, buf, &val_size), 0)
            << "FAIL : find failed / key has not been found. (" << i << "th find, " << key << " key).\n";

        int flag = db_delete(table_id, key);

        EXPECT_EQ(flag, 0)
            << "FAIL : deletion failed(" << key << ").\n";
        EXPECT_EQ(db_find(table_id, key, buf, &val_size), -1)
            << "FAIL : find failed / key has been found. (" << key << ").\n";
    }

    EXPECT_EQ(shutdown_db(), 0)
        << "FAIL : shutdown_db failed.\n";
}
