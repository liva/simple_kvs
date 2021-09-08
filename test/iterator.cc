#include "kvs/simple_kvs.h"
#include "./test.h"
#include "./kvs_misc.h"
#include <assert.h>
#include <memory>
#include <utility>

using namespace HayaguiKvs;
struct TestKvsEntryIteratorBase final : public KvsEntryIteratorBaseInterface
{
    bool *deleted_;
    TestKvsEntryIteratorBase() = delete;
    TestKvsEntryIteratorBase(bool &deleted)
    {
        deleted_ = &deleted;
    }
    virtual ~TestKvsEntryIteratorBase()
    {
        *deleted_ = true;
    }
    static KvsEntryIterator Create(bool &deleted)
    {
        return KvsEntryIterator(new TestKvsEntryIteratorBase(deleted));
    }
    virtual bool hasNext() override final
    {
        return false;
    }
    virtual KvsEntryIteratorBaseInterface *GetNext() override final
    {
        assert(false);
        return nullptr;
    }
    virtual Status Get(ReadOptions options, SliceContainer &container) override final
    {
        return Status::CreateOkStatus();
    }
    virtual Status Put(WriteOptions options, ConstSlice &value) override final
    {
        return Status::CreateOkStatus();
    }
    virtual Status Delete(WriteOptions options) override final
    {
        return Status::CreateOkStatus();
    }
    virtual Status GetKey(SliceContainer &container) override final
    {
        return Status::CreateOkStatus();
    }
    virtual void Destroy() override final
    {
        delete this;
    }
};

class DestroyTester : public Tester
{
public:
    DestroyTester(KvsAllocatorInterface &kvs_allocator) : Tester(kvs_allocator)
    {
    }
    virtual void Do() override
    {
        START_TEST;
        bool deleted = false;
        {
            KvsEntryIterator iter = TestKvsEntryIteratorBase::Create(deleted);
        }
        assert(deleted);
    }
};

class CreateTester : public Tester
{
public:
    CreateTester(KvsAllocatorInterface &kvs_allocator) : Tester(kvs_allocator)
    {
    }
    virtual void Do() override
    {
        START_TEST;
        ConstSlice key("111", 3);
        KvsEntryIterator iter = kvs_->GetIterator(key);
        (void)iter; // suppress warning
    }
};

class GetInvalidEntryTester : public Tester
{
public:
    GetInvalidEntryTester(KvsAllocatorInterface &kvs_allocator) : Tester(kvs_allocator)
    {
    }
    virtual void Do() override
    {
        START_TEST;
        ConstSlice key("111", 3);
        KvsEntryIterator iter = kvs_->GetIterator(key);
        SliceContainer container;
        assert(iter.Get(ReadOptions(), container).IsError());
    }
};

class PutGetInvalidEntryTester : public Tester
{
public:
    PutGetInvalidEntryTester(KvsAllocatorInterface &kvs_allocator) : Tester(kvs_allocator)
    {
    }
    virtual void Do() override
    {
        START_TEST;
        ConstSlice key1("111", 3);
        ConstSlice key2("111", 3);
        ConstSlice value1("abcde", 5);
        assert(kvs_->Put(WriteOptions(), key1, value1).IsOk());
        KvsEntryIterator iter = kvs_->GetIterator(key1);
        SliceContainer container;
        assert(iter.Get(ReadOptions(), container).IsOk());
        assert(container.DoesMatch(value1));
    }
};

class PutTester : public Tester
{
public:
    PutTester(KvsAllocatorInterface &kvs_allocator) : Tester(kvs_allocator)
    {
    }
    virtual void Do() override
    {
        START_TEST;
        ConstSlice value1("abcde", 5);
        {
            ConstSlice key("111", 3);
            KvsEntryIterator iter = kvs_->GetIterator(key);
            assert(iter.Put(WriteOptions(), value1).IsOk());
        }
        {
            ConstSlice key("111", 3);
            KvsEntryIterator iter = kvs_->GetIterator(key);
            SliceContainer container;
            assert(iter.Get(ReadOptions(), container).IsOk());
            assert(container.DoesMatch(value1));
        }
    }
};

class OverwriteTester : public Tester
{
public:
    OverwriteTester(KvsAllocatorInterface &kvs_allocator) : Tester(kvs_allocator)
    {
    }
    virtual void Do() override
    {
        START_TEST;
        ConstSlice value1("abcde", 5);
        ConstSlice value2("xbcdefg", 7);
        {
            ConstSlice key("111", 3);
            KvsEntryIterator iter = kvs_->GetIterator(key);
            assert(iter.Put(WriteOptions(), value1).IsOk());
        }
        {
            ConstSlice key("111", 3);
            KvsEntryIterator iter = kvs_->GetIterator(key);
            assert(iter.Put(WriteOptions(), value2).IsOk());
        }
        {
            ConstSlice key("111", 3);
            KvsEntryIterator iter = kvs_->GetIterator(key);
            SliceContainer container;
            assert(iter.Get(ReadOptions(), container).IsOk());
            assert(container.DoesMatch(value2));
        }
    }
};

class DeleteTester : public Tester
{
public:
    DeleteTester(KvsAllocatorInterface &kvs_allocator) : Tester(kvs_allocator)
    {
    }
    virtual void Do() override
    {
        START_TEST;
        ConstSlice value1("abcde", 5);
        {
            ConstSlice key("111", 3);
            KvsEntryIterator iter = kvs_->GetIterator(key);
            assert(iter.Put(WriteOptions(), value1).IsOk());
        }
        {
            ConstSlice key("111", 3);
            KvsEntryIterator iter = kvs_->GetIterator(key);
            assert(iter.Delete(WriteOptions()).IsOk());
        }
        {
            ConstSlice key("111", 3);
            KvsEntryIterator iter = kvs_->GetIterator(key);
            SliceContainer container;
            assert(iter.Get(ReadOptions(), container).IsError());
        }
    }
};

class NextTester : public Tester
{
public:
    NextTester(KvsAllocatorInterface &kvs_allocator) : Tester(kvs_allocator)
    {
    }
    virtual void Do() override
    {
        START_TEST;
        ConstSlice key1("111", 3);
        ConstSlice key2("123", 3);
        ConstSlice value("fghijkl", 7);
        {
            ConstSlice value1("abcde", 5);
            assert(kvs_->Put(WriteOptions(), key1, value1).IsOk());
        }
        {
            assert(kvs_->Put(WriteOptions(), key2, value).IsOk());
        }
        {
            KvsEntryIterator iter = kvs_->GetIterator(key1);
            Optional<KvsEntryIterator> next_iter = iter.GetNext();
            assert(next_iter.isPresent());
            iter = std::move(next_iter.get());
            SliceContainer container;
            assert(iter.Get(ReadOptions(), container).IsOk());
            assert(container.DoesMatch(value));
        }
    }
};

class JumpDeletedValueTester : public Tester
{
public:
    JumpDeletedValueTester(KvsAllocatorInterface &kvs_allocator) : Tester(kvs_allocator)
    {
    }
    virtual void Do() override
    {
        START_TEST;
        ConstSlice key1("11", 2);
        ConstSlice key2("123", 3);
        ConstSlice key3("3", 1);
        ConstSlice value1("fghijkl", 7);
        ConstSlice value2("a", 1);
        ConstSlice value3("cde", 3);
        assert(kvs_->Put(WriteOptions(), key1, value1).IsOk());
        assert(kvs_->Put(WriteOptions(), key2, value2).IsOk());
        assert(kvs_->Put(WriteOptions(), key3, value3).IsOk());
        KvsEntryIterator iter = kvs_->GetIterator(key1);
        assert(kvs_->Delete(WriteOptions(), key2).IsOk());
        Optional<KvsEntryIterator> next_iter = iter.GetNext();
        assert(next_iter.isPresent());
        iter = std::move(next_iter.get());
        SliceContainer container;
        assert(iter.Get(ReadOptions(), container).IsOk());
        assert(container.DoesMatch(value3));
    }
};


void test(KvsAllocatorInterface &kvs_allocator)
{
    {
        DestroyTester tester(kvs_allocator);
        tester.Do();
    }
    {
        CreateTester tester(kvs_allocator);
        tester.Do();
    }
    {
        GetInvalidEntryTester tester(kvs_allocator);
        tester.Do();
    }
    {
        PutGetInvalidEntryTester tester(kvs_allocator);
        tester.Do();
    }
    {
        PutTester tester(kvs_allocator);
        tester.Do();
    }
    {
        OverwriteTester tester(kvs_allocator);
        tester.Do();
    }
    {
        DeleteTester tester(kvs_allocator);
        tester.Do();
    }
    {
        NextTester tester(kvs_allocator);
        tester.Do();
    }
    {
        JumpDeletedValueTester tester(kvs_allocator);
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