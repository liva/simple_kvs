#pragma once
#include "kvs/simple_kvs.h"
#include "kvs/linkedlist.h"
#include "kvs/skiplist.h"
#include "kvs/hash.h"
#include "kvs/char_storage_kvs.h"
#include "char_storage/char_storage_over_blockstorage.h"
#include "char_storage/vefs.h"
#include "block_storage/unvme.h"
#include "block_storage/vefs.h"
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
        : block_storage_(file_.fname_), cache_kvs_(8, kvs_allocator_, hash_calculator_), char_storage_(block_storage_), kvs_(char_storage_, cache_kvs_) {}
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
    VefsFile file_;
    VefsBlockStorage block_storage_;
    SimpleHashCalculator hash_calculator_;
    HashKvs cache_kvs_;
    AppendOnlyCharStorageOverBlockStorage<GenericBlockBuffer> char_storage_;
    CharStorageKvs kvs_;
};

class CharStorageKvsContainer final : public KvsContainerInterface
{
public:
    CharStorageKvsContainer()
        : char_storage_(file_.fname_), kvs_(char_storage_, cache_kvs_) {}
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
    VefsFile file_;
    VefsCharStorage char_storage_;
    SkipListKvs<12> cache_kvs_;
    CharStorageKvs kvs_;
};
