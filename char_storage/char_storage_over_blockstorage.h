#pragma once
#include "block_storage/block_storage_multiplier.h"
#include "block_storage/block_storage_with_cache.h"
#include "char_storage/interface.h"
#include "utils/slice.h"
#include <assert.h>
#include <string.h>

namespace HayaguiKvs
{
    template <class BlockBuffer>
    class MetaDataManagerForAppendOnlyCharStorageOverBlockStorage
    {
    public:
        using MultipliedBlockStorage = typename BlockStorageMultiplier<BlockBuffer>::MultipliedBlockStorage;
        MetaDataManagerForAppendOnlyCharStorageOverBlockStorage() = delete;
        MetaDataManagerForAppendOnlyCharStorageOverBlockStorage(MultipliedBlockStorage &&storage)
            : storage_(std::move(storage))
        {
        }
        Status Open()
        {
            if (opened_)
            {
                return Status::CreateOkStatus();
            }
            if (storage_.Open().IsError())
            {
                return Status::CreateErrorStatus();
            }
            if (storage_.Read(LogicalBlockAddress(0), buf_).IsError())
            {
                return Status::CreateErrorStatus();
            }
            BlockBufferInterface &buf = buf_;
            const bool matched = (buf.Memcmp(kSignature, 0, strlen(kSignature)) == 0);
            if (!matched)
            {
                len_ = 0;

                BlockBufferInterface &buf = buf_;
                buf.CopyFrom((const uint8_t *const)kSignature, 0, strlen(kSignature));
                buf.SetValue<uint64_t>(getOffsetOfSize(), len_);
                if (storage_.Write(LogicalBlockAddress(0), buf_).IsError())
                {
                    buf.SetValue<uint64_t>(getOffsetOfSize(), len_);
                    return Status::CreateErrorStatus();
                }

                opened_ = true;
                return Status::CreateOkStatus();
            }
            len_ = buf.GetValue<uint64_t>(getOffsetOfSize());
            opened_ = true;
            return Status::CreateOkStatus();
        }
        size_t GetLen() const
        {
            return len_;
        }
        Status SetLen(size_t len)
        {
            BlockBufferInterface &buf = buf_;
            buf.SetValue<uint64_t>(getOffsetOfSize(), len);
            if (storage_.Write(LogicalBlockAddress(0), buf_).IsError())
            {
                buf.SetValue<uint64_t>(getOffsetOfSize(), len);
                return Status::CreateErrorStatus();
            }
            len_ = len;
            return Status::CreateOkStatus();
        }

    private:
        static const size_t alignUp8(const size_t value)
        {
            return ((value + 7) / 8) * 8;
        }
        static const size_t getOffsetOfSize()
        {
            return alignUp8(strlen(kSignature));
        }
        MultipliedBlockStorage storage_;
        bool opened_ = false;
        BlockBuffer buf_;
        size_t len_;
        static constexpr const char *const kSignature = "HAYAGUI_APPEND_FILE_V1_";
    };

    template <class BlockBuffer>
    class AppendOnlyCharStorageOverBlockStorage : public AppendOnlyCharStorageInterface
    {
    public:
        AppendOnlyCharStorageOverBlockStorage(BlockStorageInterface<BlockBuffer> &blockstorage)
            : multiplier_(CreateMultiplier(blockstorage)),
              metadata_manager_(CreateMetaDataManager(multiplier_)),
              data_storage_base_(multiplier_.GetMultipliedBlockStorage(kDataStorageIndex)),
              data_storage_(data_storage_base_)
        {
        }
        virtual Status Open() override
        {
            if (metadata_manager_.Open().IsError())
            {
                return Status::CreateErrorStatus();
            }
            if (data_storage_.SetCacheAddress(BlockBufferInterface::GetAddressFromOffset(GetLen())).IsError())
            {
                return Status::CreateErrorStatus();
            }
            return Status::CreateOkStatus();
        }
        virtual Status Append(const ValidSlice &slice) override
        {
            const size_t old_len = GetLen();
            const size_t len = slice.GetLen();
            const size_t new_len = old_len + len;
            const LogicalBlockAddress start = BlockBufferInterface::GetAddressFromOffset(old_len);
            const LogicalBlockAddress end = BlockBufferInterface::GetAddressFromOffset(new_len - 1);
            const LogicalBlockRegion region = LogicalBlockRegion(start, end);
            const int cnt = region.GetRegionSize();
            BlockBuffers<BlockBuffer> buffers(cnt);

            if (BlockBufferInterface::GetInBufferOffset(old_len) != 0 &&
                data_storage_.Read(start, *buffers.GetBlockBufferFromIndex(0)).IsError())
            {
                return Status::CreateErrorStatus();
            }

            BlockBufferCopierFromSlice<BlockBuffer> copier(cnt, buffers, slice, BlockBufferInterface::GetInBufferOffset(old_len));
            copier.Copy();

            if (data_storage_.SetCacheAddress(BlockBufferInterface::GetAddressFromOffset(new_len)).IsError())
            {
                return Status::CreateErrorStatus();
            }

            if (data_storage_.WriteBlocks(region, buffers).IsError())
            {
                return Status::CreateErrorStatus();
            }

            if (metadata_manager_.SetLen(new_len).IsError())
            {
                return Status::CreateErrorStatus();
            }
            return Status::CreateOkStatus();
        }
        virtual Status Read(const size_t offset, const int len, SliceContainer &container) override
        {
            const size_t file_len = GetLen();
            if (offset > file_len)
            {
                return Status::CreateErrorStatus();
            }
            const size_t actual_len = getMin((size_t)len, file_len - offset);
            return ReadInternal(offset, actual_len, container);
        }
        virtual size_t GetLen() const override
        {
            return metadata_manager_.GetLen();
        }

    private:
        using MultipliedBlockStorage = typename BlockStorageMultiplier<BlockBuffer>::MultipliedBlockStorage;
        static BlockStorageMultiplier<BlockBuffer> CreateMultiplier(BlockStorageInterface<BlockBuffer> &blockstorage)
        {
            MultiplyRule rule;
            int i;
            Status s1 = rule.AppendRegion(LogicalBlockRegion(LogicalBlockAddress(0), LogicalBlockAddress(0)), i);
            assert(s1.IsOk());
            assert(i == kMetaDataStorageIndex);
            Status s2 = rule.AppendRegion(LogicalBlockRegion(LogicalBlockAddress(1), blockstorage.GetMaxAddress()), i);
            assert(s2.IsOk());
            assert(i == kDataStorageIndex);
            return BlockStorageMultiplier<BlockBuffer>(blockstorage, rule);
        }
        static MetaDataManagerForAppendOnlyCharStorageOverBlockStorage<BlockBuffer> CreateMetaDataManager(BlockStorageMultiplier<BlockBuffer> multiplier)
        {
            MultipliedBlockStorage storage = multiplier.GetMultipliedBlockStorage(kMetaDataStorageIndex);
            return MetaDataManagerForAppendOnlyCharStorageOverBlockStorage<BlockBuffer>(std::move(storage));
        }
        Status ReadInternal(const size_t offset, const int len, SliceContainer &container)
        {
            const LogicalBlockAddress start = BlockBufferInterface::GetAddressFromOffset(offset);
            const LogicalBlockAddress end = BlockBufferInterface::GetAddressFromOffset(offset + len);
            const LogicalBlockRegion region = LogicalBlockRegion(start, end);
            const int cnt = region.GetRegionSize();
            BlockBuffers<BlockBuffer> buffers(cnt);
            if (data_storage_.ReadBlocks(region, buffers).IsError())
            {
                return Status::CreateErrorStatus();
            }

            BlockBufferCopierToSliceContainer<BlockBuffer> copier(cnt, buffers, BlockBufferInterface::GetInBufferOffset(offset), len);
            copier.Copy();
            copier.ApplyTo(container);

            return Status::CreateOkStatus();
        }
        BlockStorageMultiplier<BlockBuffer> multiplier_;
        MetaDataManagerForAppendOnlyCharStorageOverBlockStorage<BlockBuffer> metadata_manager_;
        MultipliedBlockStorage data_storage_base_;
        BlockStorageWithOneCache<BlockBuffer> data_storage_;
        static const int kMetaDataStorageIndex = 0;
        static const int kDataStorageIndex = 1;
    };
}