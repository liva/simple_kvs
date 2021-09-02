#pragma once
#include "block_storage_with_cache.h"

namespace HayaguiKvs
{
    // useful for logging
    template <class BlockBuffer>
    class BlockStorageWithOneCache : public BlockStorageInterface<BlockBuffer>
    {
    public:
        BlockStorageWithOneCache() = delete;
        BlockStorageWithOneCache(BlockStorageInterface<BlockBuffer> &underlying_blockstorage) : underlying_blockstorage_(underlying_blockstorage)
        {
        }
        virtual ~BlockStorageWithOneCache()
        {
        }
        virtual Status Open() override
        {
            return underlying_blockstorage_.Open();
        }
        virtual LogicalBlockAddress GetMaxAddress() const override
        {
            return underlying_blockstorage_.GetMaxAddress();
        }
        Status SetCacheAddress(const LogicalBlockAddress address)
        {
            if (!BlockStorageInterface<BlockBuffer>::IsValidAddress(address))
            {
                return Status::CreateErrorStatus();
            }
            if (cache_target_address_.IsEqualTo(address))
            {
                return Status::CreateOkStatus();
            }
            cache_target_address_.Set(address);
            return Status::CreateOkStatus();
        }

    private:
        class CachedAddress
        {
        public:
            void Set(LogicalBlockAddress address)
            {
                address_ = address;
                available_ = true;
            }
            bool IsEqualTo(const LogicalBlockAddress address) const
            {
                return available_ && address_.Cmp(address).IsEqual();
            }

        private:
            LogicalBlockAddress address_ = LogicalBlockAddress(0);
            bool available_ = false;
        };
        class Cache
        {
        public:
            void CopyFrom(const BlockBuffer &buffer, const LogicalBlockAddress address)
            {
                cached_address_.Set(address);
                buffer_.CopyFrom(buffer);
            }
            bool CopyToIfCached(BlockBuffer &buffer, const LogicalBlockAddress address) const
            {
                if (cached_address_.IsEqualTo(address))
                {
                    buffer.CopyFrom(buffer_);
                    return true;
                }
                return false;
            }

        private:
            BlockBuffer buffer_;
            CachedAddress cached_address_;
        } cache_;
        virtual Status ReadInternal(const LogicalBlockAddress address, BlockBuffer &buffer) override
        {
            if (cache_.CopyToIfCached(buffer, address))
            {
                return Status::CreateOkStatus();
            }
            if (underlying_blockstorage_.Read(address, buffer).IsError())
            {
                return Status::CreateErrorStatus();
            }
            if (cache_target_address_.IsEqualTo(address))
            {
                cache_.CopyFrom(buffer, address);
            }
            return Status::CreateOkStatus();
        }
        virtual Status WriteInternal(const LogicalBlockAddress address, const BlockBuffer &buffer) override
        {
            if (cache_target_address_.IsEqualTo(address))
            {
                cache_.CopyFrom(buffer, address);
            }
            return underlying_blockstorage_.Write(address, buffer);
        }
        CachedAddress cache_target_address_;
        BlockStorageInterface<BlockBuffer> &underlying_blockstorage_;
    };
}