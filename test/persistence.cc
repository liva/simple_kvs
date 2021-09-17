#include "kvs/hierarchical_kvs.h"
#include "kvs/simple_kvs.h"
#include "kvs/linkedlist.h"
#include "kvs/char_storage_kvs.h"
#include "char_storage/char_storage_over_blockstorage.h"
#include "block_storage/memblock_storage.h"
#include "block_storage/file_block_storage.h"
#include "./test.h"
#include "misc.h"
#include <assert.h>
#include <memory>
#include <utility>

using namespace HayaguiKvs;

class Tester
{
public:
    void Write(Kvs &kvs)
    {
        assert(kvs.Put(WriteOptions(), key1_, value1_).IsOk());
        assert(kvs.Put(WriteOptions(), key1_, value2_).IsOk());
        assert(kvs.Put(WriteOptions(), key3_, value3_).IsOk());
        assert(kvs.Put(WriteOptions(), key4_, value1_).IsOk());
        assert(kvs.Delete(WriteOptions(), key4_).IsOk());
    }
    void Read(Kvs &kvs)
    {
        SliceContainer container;
        assert(kvs.Get(ReadOptions(), key1_, container).IsOk());
        assert(container.DoesMatch(value2_));
        assert(kvs.Get(ReadOptions(), key3_, container).IsOk());
        assert(container.DoesMatch(value3_));
        assert(kvs.Get(ReadOptions(), key4_, container).IsError());
    }

private:
    ConstSlice key1_ = ConstSlice("111", 3);
    ConstSlice value1_ = ConstSlice("abcde", 5);
    ConstSlice value2_ = ConstSlice("defg", 4);
    ConstSlice key3_ = ConstSlice("1001", 4);
    ConstSlice value3_ = ConstSlice("xy", 2);
    ConstSlice key4_ = ConstSlice("100010", 3);
};

static inline void persist_with_underlying_kvs()
{
    START_TEST;
    SimpleKvs subkvs;
    Tester tester;
    {
        SimpleKvs base_kvs;
        HierarchicalKvs hierarchical_kvs(base_kvs, subkvs);
        tester.Write(hierarchical_kvs);
    }
    {
        SimpleKvs base_kvs;
        HierarchicalKvs hierarchical_kvs(base_kvs, subkvs);
        tester.Read(hierarchical_kvs);
    }
}

static inline void recover_from_block_storage()
{
    START_TEST;
    MemBlockStorage block_storage;
    Tester tester;
    {
        SimpleKvs cache_kvs;
        AppendOnlyCharStorageOverBlockStorage<GenericBlockBuffer> char_storage(block_storage);
        CharStorageKvs char_storage_kvs(char_storage, cache_kvs);
        tester.Write(char_storage_kvs);
    }
    {
        SimpleKvs cache_kvs;
        AppendOnlyCharStorageOverBlockStorage<GenericBlockBuffer> char_storage(block_storage);
        CharStorageKvs char_storage_kvs(char_storage, cache_kvs);
        tester.Read(char_storage_kvs);
    }
}

static inline void recover_from_file()
{
    START_TEST;
    File file;

    Tester tester;
    {
        FileBlockStorage block_storage(file.fname_);
        LinkedListKvs cache_kvs;
        AppendOnlyCharStorageOverBlockStorage<GenericBlockBuffer> char_storage(block_storage);
        CharStorageKvs char_storage_kvs(char_storage, cache_kvs);
        tester.Write(char_storage_kvs);
    }
    {
        FileBlockStorage block_storage(file.fname_);
        LinkedListKvs cache_kvs;
        AppendOnlyCharStorageOverBlockStorage<GenericBlockBuffer> char_storage(block_storage);
        CharStorageKvs char_storage_kvs(char_storage, cache_kvs);
        tester.Read(char_storage_kvs);
    }
}

static inline void store_many_kvpairs()
{
    START_TEST;
    File file;

    {
        FileBlockStorage block_storage(file.fname_);
        LinkedListKvs cache_kvs;
        AppendOnlyCharStorageOverBlockStorage<GenericBlockBuffer> char_storage(block_storage);
        CharStorageKvs char_storage_kvs(char_storage, cache_kvs);
        for (int i = 0; i < 3500; i++)
        {
            char key[20];
            snprintf(key, sizeof(key), "%016d", i);
            assert(char_storage_kvs.Put(WriteOptions(), ConstSlice(key, strlen(key)), CreateSliceFromChar('a', 500)).IsOk());
        }
    }
    {
        FileBlockStorage block_storage(file.fname_);
        LinkedListKvs cache_kvs;
        AppendOnlyCharStorageOverBlockStorage<GenericBlockBuffer> char_storage(block_storage);
        CharStorageKvs char_storage_kvs(char_storage, cache_kvs);
        for (int i = 0; i < 3500; i++)
        {
            char key[20];
            snprintf(key, sizeof(key), "%016d", i);
            SliceContainer container;
            assert(char_storage_kvs.Get(ReadOptions(), ConstSlice(key, strlen(key)), container).IsOk());
        }
    }
}

int main()
{
    persist_with_underlying_kvs();
    recover_from_block_storage();
    recover_from_file();
    store_many_kvpairs();
    return 0;
}