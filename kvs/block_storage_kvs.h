#pragma once
#include "kvs_interface.h"
#include "block_storage/block_storage_interface.h"
#include "log.h"
#include "appendonly_storage.h"

namespace HayaguiKvs
{
    template <class BlockBuffer>
    class BlockStorageKvs final : public Kvs
    {
    public:
        BlockStorageKvs() = delete;
        BlockStorageKvs(BlockStorageInterface<BlockBuffer> &underlying_storage)
            : underlying_storage_(underlying_storage),
              char_storage_(underlying_storage),
              log_(char_storage_)
        {
            if (RecoverFromUnderlyingStorage().IsError())
            {
                abort();
            }
            if (log_.Open().IsError())
            {
                abort();
            }
        }
        virtual ~BlockStorageKvs()
        {
        }
        virtual Status Get(ReadOptions options, ConstSlice &key, SliceContainer &container) override
        {
            // This is not prefarable.
            // should split Kvs Interface between ReadableKvs and WritableKvs
            abort();
        }
        virtual Status Put(WriteOptions options, ConstSlice &key, ConstSlice &value) override
        {
            if (log_.AppendEntry(key).IsError())
            {
                return Status::CreateErrorStatus();
            }
            if (log_.AppendEntry(value).IsError())
            {
                return Status::CreateErrorStatus();
            }
        }
        virtual Status Delete(WriteOptions options, ConstSlice &key) override
        {
        }
        virtual Optional<KvsEntryIterator> GetFirstIterator() override
        {
        }
        virtual KvsEntryIterator GetIterator(ConstSlice &key) override
        {
        }

    private:
        Status RecoverFromUnderlyingStorage()
        {
        }

        BlockStorageInterface<BlockBuffer> &underlying_storage_;
        AppendOnlyCharStorage<BlockBuffer> char_storage_;
        AppendOnlyLog<BlockBuffer> log_;
    };
}