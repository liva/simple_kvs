#include "kvs/simple_kvs.h"
#include "kvs/hash.h"
#include "./test.h"
#include "./kvs_misc.h"
#include <assert.h>
#include <memory>

using namespace HayaguiKvs;

class ReadWithoutWriteTester : public Tester
{
public:
    ReadWithoutWriteTester(KvsAllocatorInterface &kvs_allocator) : Tester(kvs_allocator)
    {
    }
    virtual void Do() override
    {
        START_TEST;
        ConstSlice key("111", 3);
        SliceContainer container;
        assert(kvs_->Get(ReadOptions(), key, container).IsError());
    }
};

class WriteAndReadOnceTester : public Tester
{
public:
    WriteAndReadOnceTester(KvsAllocatorInterface &kvs_allocator) : Tester(kvs_allocator)
    {
    }
    virtual void Do() override
    {
        START_TEST;
        ConstSlice key1("111", 3);
        ConstSlice key2("111", 3);
        ConstSlice value1("abcde", 5);
        assert(kvs_->Put(WriteOptions(), key1, value1).IsOk());
        SliceContainer container;
        assert(kvs_->Get(ReadOptions(), key2, container).IsOk());
        assert(container.DoesMatch(value1));
    }
};

class WriteTwiceThenReadTester : public Tester
{
public:
    WriteTwiceThenReadTester(KvsAllocatorInterface &kvs_allocator) : Tester(kvs_allocator)
    {
    }
    virtual void Do() override
    {
        START_TEST;
        ConstSlice key("123", 3);
        ConstSlice value1("abcde", 5);
        ConstSlice value2("defg", 4);
        assert(kvs_->Put(WriteOptions(), key, value1).IsOk());
        assert(kvs_->Put(WriteOptions(), key, value2).IsOk());
        SliceContainer container;
        assert(kvs_->Get(ReadOptions(), key, container).IsOk());
        assert(container.DoesMatch(value2));
    }
};

class DeleteAndReadTester : public Tester
{
public:
    DeleteAndReadTester(KvsAllocatorInterface &kvs_allocator) : Tester(kvs_allocator)
    {
    }
    virtual void Do() override
    {
        START_TEST;
        ConstSlice key1("111", 3);
        ConstSlice key2("111", 3);
        ConstSlice key3("111", 3);
        ConstSlice value1("abcde", 5);
        assert(kvs_->Put(WriteOptions(), key1, value1).IsOk());
        assert(kvs_->Delete(WriteOptions(), key2).IsOk());
        SliceContainer container;
        assert(kvs_->Get(ReadOptions(), key3, container).IsError());
    }
};

class WriteSortedOrderTester : public Tester
{
public:
    WriteSortedOrderTester(KvsAllocatorInterface &kvs_allocator) : Tester(kvs_allocator)
    {
    }
    virtual void Do() override
    {
        START_TEST;
        ConstSlice key1("10000", 1);
        ConstSlice key2("100010", 3);
        ConstSlice key3("13", 4);
        ConstSlice value1("abcde", 5);
        ConstSlice value2("fghi", 4);
        ConstSlice value3("xy", 2);
        assert(kvs_->Put(WriteOptions(), key1, value1).IsOk());
        assert(kvs_->Put(WriteOptions(), key2, value2).IsOk());
        assert(kvs_->Put(WriteOptions(), key3, value3).IsOk());
        SliceContainer container;
        assert(kvs_->Get(ReadOptions(), key1, container).IsOk());
        assert(container.DoesMatch(value1));
        assert(kvs_->Get(ReadOptions(), key2, container).IsOk());
        assert(container.DoesMatch(value2));
        assert(kvs_->Get(ReadOptions(), key3, container).IsOk());
        assert(container.DoesMatch(value3));
    }
};

class WriteUnsortedOrderTester : public Tester
{
public:
    WriteUnsortedOrderTester(KvsAllocatorInterface &kvs_allocator) : Tester(kvs_allocator)
    {
    }
    virtual void Do() override
    {
        START_TEST;
        ConstSlice key1("3", 1);
        ConstSlice key2("132", 3);
        ConstSlice key3("1214", 4);
        ConstSlice value1("abcde", 5);
        ConstSlice value2("fghi", 4);
        ConstSlice value3("xy", 2);
        assert(kvs_->Put(WriteOptions(), key1, value1).IsOk());
        assert(kvs_->Put(WriteOptions(), key2, value2).IsOk());
        assert(kvs_->Put(WriteOptions(), key3, value3).IsOk());
        SliceContainer container;
        assert(kvs_->Get(ReadOptions(), key1, container).IsOk());
        assert(container.DoesMatch(value1));
        assert(kvs_->Get(ReadOptions(), key2, container).IsOk());
        assert(container.DoesMatch(value2));
        assert(kvs_->Get(ReadOptions(), key3, container).IsOk());
        assert(container.DoesMatch(value3));
    }
private:
};

void test(KvsAllocatorInterface &kvs_allocator)
{
    {
        ReadWithoutWriteTester tester(kvs_allocator);
        tester.Do();
    }
    {
        WriteAndReadOnceTester tester(kvs_allocator);
        tester.Do();
    }
    {
        WriteTwiceThenReadTester tester(kvs_allocator);
        tester.Do();
    }
    {
        DeleteAndReadTester tester(kvs_allocator);
        tester.Do();
    }
    {
        WriteSortedOrderTester tester(kvs_allocator);
        tester.Do();
    }
    {
        WriteUnsortedOrderTester tester(kvs_allocator);
        tester.Do();
    }
}

int main()
{
    {
        GenericKvsAllocator<SimpleKvs> kvs_allocator;
        test(kvs_allocator);
    }
    {
        HashKvsAllocator kvs_allocator;
        test(kvs_allocator);
    }
    return 0;
}
