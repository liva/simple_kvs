#pragma once
#include "kvs_interface.h"
#include <memory>
#include "block_storage_interface.h"

namespace HayaguiKvs
{
    template <class BlockBuffer>
    class BlockStorageKvs : public Kvs
    {
    public:
        BlockStorageKvs() = delete;
        BlockStorageKvs(std::shared_ptr<BlockStorageInterface<BlockBuffer>> underlying_storage) : underlying_storage_(underlying_storage)
        {
        }
        virtual ~BlockStorageKvs()
        {
        }
        virtual Status Get(ReadOptions options, ConstSlice &key, SliceContainer &container) = 0;
        virtual Status Put(WriteOptions options, ConstSlice &key, ConstSlice &value) = 0;
        virtual Status Delete(WriteOptions options, ConstSlice &key) = 0;
        virtual Optional<KvsEntryIterator> GetFirstIterator() = 0;
        virtual KvsEntryIterator GetIterator(ConstSlice &key) = 0;
    private:
        std::shared_ptr<BlockStorageInterface<BlockBuffer>> underlying_storage_;
    };
}