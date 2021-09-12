#pragma once
#include "kvs/simple_kvs.h"
#include "kvs/linkedlist.h"
#include "kvs/hash.h"
#include "kvs/block_storage_kvs.h"
#include "block_storage/unvme.h"
#include "misc.h"

using namespace HayaguiKvs;
class HashKvsContainer final : public KvsContainerInterface
{
public:
    HashKvsContainer() : kvs_(8, kvs_allocator_, hash_calculator_) {}
    virtual Kvs *operator->() override
    {
        return &kvs_;
    }

private:
    class Allocator : public KvsAllocatorInterface
    {
        virtual Kvs *Allocate() override
        {
            return new LinkedListKvs();
        }
    } kvs_allocator_;
    SimpleHashCalculator hash_calculator_;
    HashKvs kvs_;
};

class BlockStoragKvsContainer final : public KvsContainerInterface
{
public:
    BlockStoragKvsContainer()
        : cache_kvs_(8, kvs_allocator_, hash_calculator_), kvs_(block_storage_, cache_kvs_) {}
    virtual Kvs *operator->() override
    {
        return &kvs_;
    }

private:
    class Allocator : public KvsAllocatorInterface
    {
        virtual Kvs *Allocate() override
        {
            return new LinkedListKvs();
        }
    } kvs_allocator_;
    UnvmeBlockStorage block_storage_;
    SimpleHashCalculator hash_calculator_;
    HashKvs cache_kvs_;
    BlockStorageKvs<GenericBlockBuffer> kvs_;
};

class BlockStoragKvsContainer1 final : public KvsContainerInterface
{
public:
    BlockStoragKvsContainer1()
        : kvs_(block_storage_, cache_kvs_) {}
    virtual Kvs *operator->() override
    {
        return &kvs_;
    }

private:
    UnvmeBlockStorage block_storage_;
    SimpleKvs cache_kvs_;
    BlockStorageKvs<GenericBlockBuffer> kvs_;
};

class BlockStoragKvsContainer2 final : public KvsContainerInterface
{
public:
    BlockStoragKvsContainer2()
        : kvs_(block_storage_, cache_kvs_) {}
    virtual Kvs *operator->() override
    {
        return &kvs_;
    }

private:
    UnvmeBlockStorage block_storage_;
    LinkedListKvs cache_kvs_;
    BlockStorageKvs<GenericBlockBuffer> kvs_;
};