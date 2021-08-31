#pragma once
#include "block_storage/block_storage_multiplier.h"
#include "utils/slice.h"
#include "utils/allocator.h"
#include <assert.h>
#include <string.h>
#include <new>

namespace HayaguiKvs
{
    struct RandomReadStorageInterface
    {
        virtual ~RandomReadStorageInterface() = 0;
        virtual Status Open() = 0;
        virtual Status Read(const size_t offset, const int len, SliceContainer &container) = 0;
        virtual size_t GetLen() const = 0;
    };
    inline RandomReadStorageInterface::~RandomReadStorageInterface() {}

    struct SequentialReadStorageInterface
    {
        virtual ~SequentialReadStorageInterface() = 0;
        virtual Status Open() = 0;
        virtual void SeekTo(const size_t offset) = 0;
        virtual Status Read(SliceContainer &container, const int len) = 0;
        virtual size_t GetLen() const = 0;
    };
    inline SequentialReadStorageInterface::~SequentialReadStorageInterface() {}

    class SequentialReadStorageOverRandomReadStorage : public SequentialReadStorageInterface
    {
    public:
        SequentialReadStorageOverRandomReadStorage(RandomReadStorageInterface &underlying_storage) : underlying_storage_(underlying_storage)
        {
        }
        virtual Status Open() override
        {
            if (underlying_storage_.Open().IsError()) {
                return Status::CreateErrorStatus();
            }
            return Status::CreateOkStatus();
        }
        virtual void SeekTo(const size_t offset) override
        {
            offset_ = offset;
        }
        virtual Status Read(SliceContainer &container, const int len) override
        {
            if (underlying_storage_.Read(offset_, len, container).IsError())
            {
                return Status::CreateErrorStatus();
            }
            int slice_len;
            assert(container.GetLen(slice_len).IsOk());
            offset_ += slice_len;
            return Status::CreateOkStatus();
        }
        virtual size_t GetLen() const override
        {
            return underlying_storage_.GetLen();
        }

    private:
        size_t offset_ = 0;
        RandomReadStorageInterface &underlying_storage_;
    };

    template <class BlockBuffer>
    class AppendOnlyCharStorage : public RandomReadStorageInterface
    {
    public:
        AppendOnlyCharStorage(BlockStorageInterface<BlockBuffer> &blockstorage)
            : multiplier_(CreateMultiplier(blockstorage)),
              metadata_manager_(CreateMetaDataManager(multiplier_)),
              data_storage_(multiplier_.GetMultipliedBlockStorage(kDataStorageIndex))
        {
        }
        virtual Status Open() override
        {
            if (metadata_manager_.Open().IsError()) {
                return Status::CreateErrorStatus();
            }
            return Status::CreateOkStatus();
        }
        Status Append(const ConstSlice &slice)
        {
            // set length
            return Status::CreateErrorStatus();
        }
        virtual Status Read(const size_t offset, const int len, SliceContainer &container) override
        {
            if (offset > metadata_manager_.GetLen())
            {
                return Status::CreateErrorStatus();
            }
            const int actual_len = offset + len > metadata_manager_.GetLen() ? metadata_manager_.GetLen() - offset : len;
            return ReadInternal(offset, actual_len, container);
        }
        virtual size_t GetLen() const override
        {
            return metadata_manager_.GetLen();
        }

    private:
        using MultipliedBlockStorage = typename BlockStorageMultiplier<BlockBuffer>::MultipliedBlockStorage;
        class MetaDataManager
        {
        public:
            MetaDataManager() = delete;
            MetaDataManager(MultipliedBlockStorage &&storage)
                : storage_(std::move(storage))
            {
            }
            Status Open()
            {
                if (opened_)
                {
                    return Status::CreateOkStatus();
                }
                if (storage_.Open().IsError()) {
                    return Status::CreateErrorStatus();
                }
                if (storage_.Read(LogicalBlockAddress(0), buf_).IsError())
                {
                    return Status::CreateErrorStatus();
                }
                BlockBufferInterface &buf = buf_;
                const bool matched = (memcmp(buf.GetPtrToTheBuffer(), kSignature, strlen(kSignature)) == 0);
                if (!matched)
                {
                    len_ = 0;
                    opened_ = true;
                    return Status::CreateOkStatus();
                }
                len_ = *GetPtrToLenInTheBuffer();
                opened_ = true;
                return Status::CreateOkStatus();
            }
            size_t GetLen() const
            {
                return len_;
            }
            Status SetLen(size_t len)
            {
                *GetPtrToLenInTheBuffer() = len;
                if (storage_.Write(LogicalBlockAddress(0), buf_).IsError())
                {
                    *GetPtrToLenInTheBuffer() = len_;
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
            uint64_t *GetPtrToLenInTheBuffer()
            {
                const size_t size_offset = getOffsetOfSize();
                BlockBufferInterface &buf = buf_;
                return reinterpret_cast<uint64_t *>(buf.GetPtrToTheBuffer() + size_offset);
            }
            MultipliedBlockStorage storage_;
            bool opened_ = false;
            BlockBuffer buf_;
            size_t len_;
            static constexpr const char *const kSignature = "HAYAGUI_APPEND_FILE_V1_";
        };
        static BlockStorageMultiplier<BlockBuffer> CreateMultiplier(BlockStorageInterface<BlockBuffer> &blockstorage)
        {
            MultiplyRule rule;
            int i;
            assert(rule.AppendRegion(LogicalBlockRegion(LogicalBlockAddress(0), LogicalBlockAddress(0)), i).IsOk());
            assert(i == kMetaDataStorageIndex);
            assert(rule.AppendRegion(LogicalBlockRegion(LogicalBlockAddress(1), blockstorage.GetMaxAddress()), i).IsOk());
            assert(i == kDataStorageIndex);
            return BlockStorageMultiplier<BlockBuffer>(blockstorage, rule);
        }
        static MetaDataManager CreateMetaDataManager(BlockStorageMultiplier<BlockBuffer> multiplier)
        {
            MultipliedBlockStorage storage = multiplier.GetMultipliedBlockStorage(kMetaDataStorageIndex);
            return MetaDataManager(std::move(storage));
        }
        Status ReadInternal(const size_t offset, const int len, SliceContainer &container)
        {
            LogicalBlockAddress start = BlockBufferInterface::GetAddressFromOffset(offset);
            LogicalBlockAddress end = BlockBufferInterface::GetAddressFromOffset(offset + len);
            int cnt = end - start;
            BlockBuffer buffers[cnt];
            for (int i = 0; i < cnt; i++)
            {
                new (&buffers[i]) BlockBuffer();
            }
            if (data_storage_.ReadBlocks(LogicalBlockRegion(start, end), buffers).IsError())
            {
                return Status::CreateErrorStatus();
            }

            container.Set(CopyDataFromBlockBuffersToConstSlice(reinterpret_cast<char *>(MemAllocator::alloc(len)), buffers, BlockBufferInterface::GetInBufferOffset(offset), len));

            for (int i = 0; i < cnt; i++)
            {
                buffers[i].~BlockBuffer();
            }
            return Status::CreateOkStatus();
        }
        ConstSlice CopyDataFromBlockBuffersToConstSlice(char *const buf, BlockBuffer *buffers, const size_t offset, const size_t len)
        {
            CopyDataFromFirstBlockBufferToBuffer(buf, buffers, offset, len);
            return ConstSlice(buf, len);
        }
        void CopyDataFromFirstBlockBufferToBuffer(char *const buf, BlockBuffer *buffers, const size_t offset, const size_t len)
        {
            if (len == 0)
            {
                return;
            }
            const size_t cur_len = getMax(offset + len, BlockBufferInterface::kSize) - offset;
            memcpy(buf, buffers[0].GetPtrToTheBuffer() + offset, cur_len);
            return CopyDataFromFirstBlockBufferToBuffer(buf + cur_len, buffers + 1, 0, len - cur_len);
        }
        template <class T>
        static T getMax(const T a, const T b)
        {
            return a > b ? a : b;
        }
        BlockStorageMultiplier<BlockBuffer> multiplier_;
        MetaDataManager metadata_manager_;
        MultipliedBlockStorage data_storage_;
        static const int kMetaDataStorageIndex = 0;
        static const int kDataStorageIndex = 1;
    };
}