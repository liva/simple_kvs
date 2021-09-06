#pragma once
#include "kvs_interface.h"
#include "block_storage/block_storage_interface.h"
#include "kvs/simple_kvs.h"
#include "log.h"
#include "appendonly_storage.h"

namespace HayaguiKvs
{
    template <class BlockBuffer>
    class BlockStorageKvs final : public Kvs
    {
    public:
        BlockStorageKvs() = delete;
        BlockStorageKvs(BlockStorageInterface<BlockBuffer> &underlying_storage, Kvs &cache_kvs)
            : underlying_storage_(underlying_storage),
              char_storage_(underlying_storage),
              log_(char_storage_),
              cache_kvs_(cache_kvs)
        {
            if (log_.Open().IsError())
            {
                abort();
            }
            RecoverFromStorage();
        }
        virtual ~BlockStorageKvs()
        {
        }
        virtual Status Get(ReadOptions options, const ConstSlice &key, SliceContainer &container) override
        {
            return cache_kvs_.Get(options, key, container);
        }
        virtual Status Put(WriteOptions options, const ConstSlice &key, const ConstSlice &value) override
        {
            if (Signature::StorePutSignature(log_).IsError())
            {
                return Status::CreateErrorStatus();
            }
            if (log_.AppendEntry(key).IsError())
            {
                return Status::CreateErrorStatus();
            }
            if (log_.AppendEntry(value).IsError())
            {
                return Status::CreateErrorStatus();
            }
            return cache_kvs_.Put(options, key, value);
        }
        virtual Status Delete(WriteOptions options, const ConstSlice &key) override
        {
            if (Signature::StoreDeleteSignature(log_).IsError())
            {
                return Status::CreateErrorStatus();
            }
            if (log_.AppendEntry(key).IsError())
            {
                return Status::CreateErrorStatus();
            }
            return cache_kvs_.Delete(options, key);
        }
        virtual Optional<KvsEntryIterator> GetFirstIterator() override
        {
            Optional<KvsEntryIterator> cache_iter = cache_kvs_.GetFirstIterator();
            if (!cache_iter.isPresent()) {
                return Optional<KvsEntryIterator>::CreateInvalidObj();
            }
            SliceContainer key_container;
            Status s1 = cache_iter.get().GetKey(key_container);
            assert(s1.IsOk());
            return Optional<KvsEntryIterator>::CreateValidObj(GetIterator(key_container.CreateConstSlice()));
        }
        virtual KvsEntryIterator GetIterator(const ConstSlice &key) override
        {
            GenericKvsEntryIteratorBase *base = MemAllocator::alloc<GenericKvsEntryIteratorBase>();
            new (base) GenericKvsEntryIteratorBase(*this, key);
            return KvsEntryIterator(base);
        }
        virtual Status FindNextKey(const ConstSlice &key, SliceContainer &container) override {
            return cache_kvs_.FindNextKey(key, container);
        }

    private:
        void RecoverFromStorage()
        {
            SequentialReadCharStorageOverRandomReadCharStorage seqread_char_storage(char_storage_);
            LogReader log(seqread_char_storage);
            if (log.Open().IsError())
            {
                return;
            }
            while (true)
            {
                uint8_t signature;
                if (Signature::RetrieveSignature(log, signature).IsError())
                {
                    return;
                }
                if (signature == Signature::kSignaturePut)
                {
                    SliceContainer key_container, value_container;
                    if (RetrievePutItem(log, key_container, value_container).IsError())
                    {
                        abort();
                    }
                    if (cache_kvs_.Put(WriteOptions(), key_container.CreateConstSlice(), value_container.CreateConstSlice()).IsError()) {
                        abort();
                    }
                }
                else if (signature == Signature::kSignatureDelete)
                {
                    SliceContainer key_container;
                    if (RetrieveDeletedItem(log, key_container).IsError())
                    {
                        abort();
                    }
                    if (cache_kvs_.Delete(WriteOptions(), key_container.CreateConstSlice()).IsError()) {
                        abort();
                    }
                }
                else
                {
                    abort();
                }
            }
        }
        Status RetrievePutItem(LogReader &log, SliceContainer &key_container, SliceContainer &value_container)
        {
            if (log.RetrieveNextEntry(key_container).IsError())
            {
                return Status::CreateErrorStatus();
            }
            return log.RetrieveNextEntry(value_container);
        }
        Status RetrieveDeletedItem(LogReader &log, SliceContainer &key_container)
        {
            return log.RetrieveNextEntry(key_container);
        }

        BlockStorageInterface<BlockBuffer> &underlying_storage_;
        AppendOnlyCharStorage<BlockBuffer> char_storage_;
        LogAppender log_;
        Kvs &cache_kvs_;

        class Signature
        {
        public:
            static Status StoreDeleteSignature(LogAppender &log)
            {
                const uint8_t value = kSignatureDelete;
                ConstSlice signature((const char *)&value, 1);
                return log.AppendEntry(signature);
            }
            static Status StorePutSignature(LogAppender &log)
            {
                const uint8_t value = kSignaturePut;
                ConstSlice signature((const char *)&value, 1);
                return log.AppendEntry(signature);
            }
            static Status RetrieveSignature(LogReader &log, uint8_t &signature)
            {
                SliceContainer signature_container;
                if (log.RetrieveNextEntry(signature_container).IsError())
                {
                    return Status::CreateErrorStatus();
                }
                int len;
                if (signature_container.GetLen(len).IsError()) {
                    return Status::CreateErrorStatus();
                }
                if (len != 1)
                {
                    return Status::CreateErrorStatus();
                }
                return signature_container.CopyToBuffer((char *)&signature);
            }
            static const uint8_t kSignaturePut = 0;
            static const uint8_t kSignatureDelete = 1;
        };
    };
}