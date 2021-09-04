#include "kvs/hierarchical_kvs.h"
#include "kvs/simple_kvs.h"
#include "kvs/block_storage_kvs.h"
#include "block_storage/memblock_storage.h"
#include "./test.h"
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
        BlockStorageKvs<GenericBlockBuffer> block_storage_kvs(block_storage, cache_kvs);
        tester.Write(block_storage_kvs);
    }
    {
        SimpleKvs cache_kvs;
        BlockStorageKvs<GenericBlockBuffer> block_storage_kvs(block_storage, cache_kvs);
        tester.Read(block_storage_kvs);
    }
}

void persistence_main()
{
    persist_with_underlying_kvs();
    recover_from_block_storage();
}