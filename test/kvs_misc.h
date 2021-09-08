#pragma once
#include "kvs/simple_kvs.h"
#include "kvs/hash.h"

using namespace HayaguiKvs;
class HashKvsAllocator final : public KvsAllocatorInterface
{
public:
    virtual Kvs *Allocate() override
    {
        return new HashKvs(8, kvs_allocator_, hash_calculator_);
    }
    virtual void Release(Kvs *kvs) override
    {
        delete kvs;
    }
    GenericKvsAllocator<SimpleKvs> kvs_allocator_;
    SimpleHashCalculator hash_calculator_;
};

class Tester
{
public:
    Tester(KvsAllocatorInterface &kvs_allocator) : kvs_allocator_(kvs_allocator), kvs_(kvs_allocator_.Allocate())
    {
    }
    virtual ~Tester()
    {
        kvs_allocator_.Release(kvs_);
    }
    virtual void Do() = 0;

protected:
    KvsAllocatorInterface &kvs_allocator_;
    Kvs *kvs_;
};