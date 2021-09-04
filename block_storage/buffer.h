#pragma once
#include "lba.h"
#include "utils/math.h"
#include "utils/allocator.h"
#include "utils/slice.h"
#include <string.h>
#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <new>

namespace HayaguiKvs
{
    // Clients read/write data from/to this buffer.
    // Each storage class can decide the way to allocate buffer.
    class BlockBufferInterface
    {
    public:
        virtual ~BlockBufferInterface() = 0;
        static LogicalBlockAddress GetAddressFromOffset(const size_t offset)
        {
            return LogicalBlockAddress(offset / kSize);
        }
        static size_t GetInBufferOffset(const size_t offset)
        {
            return offset % kSize;
        }
        void CopyFrom(const BlockBufferInterface &buffer)
        {
            memcpy(GetPtrToTheBuffer(), buffer.GetConstPtrToTheBuffer(), kSize);
        }
        void CopyFrom(const uint8_t *const buffer, size_t offset, size_t len)
        {
            assert(offset + len <= kSize);
            memcpy(GetPtrToTheBuffer() + offset, buffer, len);
        }
        void CopyFrom(const ValidSlice &slice, size_t offset)
        {
            assert(offset + slice.GetLen() <= kSize);
            assert(slice.CopyToBuffer((char *)GetPtrToTheBuffer() + offset).IsOk());
        }
        void CopyTo(uint8_t *const buffer, size_t offset, size_t len) const
        {
            assert(offset + len <= kSize);
            memcpy(buffer, GetConstPtrToTheBuffer() + offset, len);
        }
        int Memcmp(const char *const buf, size_t offset, const size_t len) const
        {
            assert(len <= kSize);
            return memcmp(GetConstPtrToTheBuffer() + offset, buf, len);
        }
        template <class T>
        T GetValue(const size_t offset) const
        {
            assert((offset % sizeof(T)) == 0);
            assert(offset + sizeof(T) <= kSize);
            return *reinterpret_cast<const T *>(GetConstPtrToTheBuffer() + offset);
        }
        template <class T>
        void SetValue(const size_t offset, T value)
        {
            assert((offset % sizeof(T)) == 0);
            assert(offset + sizeof(T) <= kSize);
            *reinterpret_cast<T *>(GetPtrToTheBuffer() + offset) = value;
        }
        void Dump() const
        {
            printf("[");
            for (int i = 0; i < kSize; i++)
            {
                printf("%02x ", GetConstPtrToTheBuffer()[i]);
            }
            printf("]\n");
        }
        static const size_t kSize = 512;

    protected:
        virtual uint8_t *GetPtrToTheBuffer() = 0;
        virtual const uint8_t *GetConstPtrToTheBuffer() const = 0;
    };
    BlockBufferInterface::~BlockBufferInterface()
    {
    }

    template <class BlockBuffer>
    class BlockBuffers
    {
    public:
        BlockBuffers(const int cnt) : cnt_(cnt), buffers_(CreateBuffers(cnt))
        {
        }
        ~BlockBuffers()
        {
            for (int i = 0; i < cnt_; i++)
            {
                buffers_[i]->~BlockBuffer();
                MemAllocator::free(buffers_[i]);
            }
            MemAllocator::free((char *)buffers_);
        }
        BlockBuffer *GetBlockBufferFromIndex(const int index)
        {
            assert(index < cnt_);
            return buffers_[index];
        }
        const BlockBuffer *GetConstBlockBufferFromIndex(const int index) const
        {
            assert(index < cnt_);
            return buffers_[index];
        }
        const int GetCnt() const
        {
            return cnt_;
        }

    private:
        using BlockBufferPtr = BlockBuffer *;
        const BlockBufferPtr *const CreateBuffers(const int cnt)
        {
            BlockBufferPtr *const buffers = (BlockBufferPtr *)MemAllocator::alloc(sizeof(BlockBufferPtr) * cnt);
            for (int i = 0; i < cnt_; i++)
            {
                buffers[i] = (BlockBufferPtr)MemAllocator::alloc(sizeof(BlockBuffer));
                new (buffers[i]) BlockBuffer();
            }
            return buffers;
        }
        const int cnt_;
        const BlockBufferPtr *const buffers_;
    };

    class BlockBufferCopierInterface
    {
    public:
        BlockBufferCopierInterface(const int buffer_cnt, const size_t offset_in_block_buffer, const size_t len)
            : buffer_cnt_(buffer_cnt),
              offset_in_block_buffer_(OffsetInBlockBuffer(offset_in_block_buffer)),
              len_(len)
        {
            assert(len_ <= buffer_cnt_ * BlockBufferInterface::kSize);
        }
        virtual ~BlockBufferCopierInterface() = 0;
        virtual void Copy()
        {
            CopyOneBlock(BlockBufferIndex(0, *this), offset_in_block_buffer_, RegionInBuffer(0, len_, *this));
        }

    protected:
        struct BlockBufferIndex
        {
            explicit BlockBufferIndex(const int value, const BlockBufferCopierInterface &copier)
                : value_(value),
                  copier_(copier)
            {
                assert(value < copier.buffer_cnt_);
            }
            const BlockBufferIndex Inc() const
            {
                return BlockBufferIndex(value_ + 1, copier_);
            }
            const int value_;
            const BlockBufferCopierInterface &copier_;
        };
        struct OffsetInBlockBuffer
        {
            explicit OffsetInBlockBuffer(const size_t value) : value_(value)
            {
                assert(value < BlockBufferInterface::kSize);
            }
            const size_t value_;
        };
        struct RegionInBuffer
        {
            explicit RegionInBuffer(const size_t offset, const size_t len, const BlockBufferCopierInterface &copier)
                : offset_(offset),
                  len_(len),
                  copier_(copier)
            {
                assert(offset + len <= copier_.len_);
            }
            const RegionInBuffer Add(const size_t len) const
            {
                return RegionInBuffer(offset_ + len, len_ - len, copier_);
            }
            const RegionInBuffer Shrink(const size_t len) const
            {
                return RegionInBuffer(offset_, len, copier_);
            }
            const size_t offset_;
            const size_t len_;
            const BlockBufferCopierInterface &copier_;
        };
        virtual void CopyInternal(const BlockBufferIndex index, const OffsetInBlockBuffer offset_in_block_buffer, const RegionInBuffer region_in_buffer) = 0;

        const int buffer_cnt_;
        const OffsetInBlockBuffer offset_in_block_buffer_;
        const size_t len_;
    private:
        virtual void CopyOneBlock(const BlockBufferIndex index, const OffsetInBlockBuffer offset_in_block_buffer, const RegionInBuffer region_in_buffer)
        {
            const size_t cur_len = getMin(region_in_buffer.len_, BlockBufferInterface::kSize - offset_in_block_buffer.value_);
            CopyInternal(index, offset_in_block_buffer, region_in_buffer.Shrink(cur_len));
            const RegionInBuffer next_region = region_in_buffer.Add(cur_len);
            if (next_region.len_ == 0)
            {
                return;
            }
            CopyOneBlock(index.Inc(), OffsetInBlockBuffer(0), next_region);
        }
    };
    BlockBufferCopierInterface::~BlockBufferCopierInterface()
    {
    }
    template <class BlockBuffer>
    class BlockBufferCopierToSliceContainer : public BlockBufferCopierInterface
    {
    public:
        BlockBufferCopierToSliceContainer() = delete;
        BlockBufferCopierToSliceContainer(const int buffer_cnt, const BlockBuffers<BlockBuffer> &buffers, const size_t offset_in_block_buffer, const size_t len)
            : BlockBufferCopierInterface(buffer_cnt, offset_in_block_buffer, len),
              buffers_(buffers),
              buf_(MemAllocator::alloc(len))
        {
        }
        ~BlockBufferCopierToSliceContainer() {
            MemAllocator::free(buf_);
        }
        void ApplyTo(SliceContainer &container) {
            container.Set(buf_, len_);
        }
    private:
        virtual void CopyInternal(const BlockBufferIndex index, const OffsetInBlockBuffer offset_in_block_buffer, const RegionInBuffer region_in_buffer) override
        {
            buffers_.GetConstBlockBufferFromIndex(index.value_)->CopyTo(reinterpret_cast<uint8_t *const>(buf_ + region_in_buffer.offset_), offset_in_block_buffer.value_, region_in_buffer.len_);
        }
        const BlockBuffers<BlockBuffer> &buffers_;
        char *buf_;
    };
    template <class BlockBuffer>
    class BlockBufferCopierFromSlice : public BlockBufferCopierInterface
    {
    public:
        BlockBufferCopierFromSlice() = delete;
        BlockBufferCopierFromSlice(const int buffer_cnt, BlockBuffers<BlockBuffer> &buffers, const ValidSlice &slice, const size_t offset_in_block_buffer)
            : BlockBufferCopierInterface(buffer_cnt, offset_in_block_buffer, slice.GetLen()),
              buffers_(buffers),
              slice_(slice)
        {
        }

    private:
        virtual void CopyInternal(const BlockBufferIndex index, const OffsetInBlockBuffer offset_in_block_buffer, const RegionInBuffer region_in_buffer) override
        {
            ShrinkedSlice slice(slice_, region_in_buffer.offset_, region_in_buffer.len_);
            buffers_.GetBlockBufferFromIndex(index.value_)->CopyFrom(slice, offset_in_block_buffer.value_);
        }
        BlockBuffers<BlockBuffer> &buffers_;
        const ValidSlice &slice_;
    };

    class GenericBlockBuffer : public BlockBufferInterface
    {
    public:
        GenericBlockBuffer()
        {
            buf_ = allocator_.AllocateBuffer();
        }
        GenericBlockBuffer(const GenericBlockBuffer &obj) = delete;
        GenericBlockBuffer(GenericBlockBuffer &&obj)
        {
            buf_ = obj.buf_;
            MarkUsedFlagToMovedObj(std::move(obj));
        }
        ~GenericBlockBuffer()
        {
            DestroyBuffer();
        }
        GenericBlockBuffer &operator=(const GenericBlockBuffer &obj) = delete;
        GenericBlockBuffer &operator=(GenericBlockBuffer &&obj)
        {
            DestroyBuffer();
            buf_ = obj.buf_;
            MarkUsedFlagToMovedObj(std::move(obj));
            return *this;
        }

    private:
        virtual uint8_t *GetPtrToTheBuffer() override
        {
            return buf_;
        }
        virtual const uint8_t *GetConstPtrToTheBuffer() const override
        {
            return buf_;
        }
        void MarkUsedFlagToMovedObj(GenericBlockBuffer &&obj)
        {
            obj.buf_ = nullptr;
        }
        void DestroyBuffer()
        {
            if (buf_)
            {
                allocator_.ReleaseBuffer(buf_);
            }
        }
        struct GenericBlockBufferAllocator
        {
            uint8_t *AllocateBuffer()
            {
                return reinterpret_cast<uint8_t *>(malloc(kSize));
            }
            void ReleaseBuffer(uint8_t *buf)
            {
                free(buf);
            }
        } allocator_;
        uint8_t *buf_;
    };
}