#pragma once
#include "utils/status.h"
#include "utils/cmp.h"
#include "utils/optional.h"
#include "utils/ptr_container.h"
#include <stddef.h>
#include <stdint.h>

namespace HayaguiKvs
{
    class LogicalBlockAddress
    {
    public:
        LogicalBlockAddress() = delete;
        explicit LogicalBlockAddress(const int raw_address) : raw_address_(raw_address)
        {
        }
        LogicalBlockAddress(const LogicalBlockAddress &obj) : raw_address_(obj.raw_address_){
        }
        CmpResult Cmp(const LogicalBlockAddress address) const
        {
            return CmpResult(raw_address_ - address.raw_address_);
        }
        int GetRaw() const
        {
            return raw_address_;
        }
        static LogicalBlockAddress GetLower(const LogicalBlockAddress address1, const LogicalBlockAddress address2)
        {
            return LogicalBlockAddress(address1.raw_address_ < address2.raw_address_ ? address1.raw_address_ : address2.raw_address_);
        }
        static LogicalBlockAddress GetGreater(const LogicalBlockAddress address1, const LogicalBlockAddress address2)
        {
            return LogicalBlockAddress(address1.raw_address_ < address2.raw_address_ ? address2.raw_address_ : address1.raw_address_);
        }
        friend LogicalBlockAddress operator+(const LogicalBlockAddress &a, const LogicalBlockAddress &b);
        friend int operator-(const LogicalBlockAddress &a, const LogicalBlockAddress &b);
        LogicalBlockAddress& operator++() {
            raw_address_++;
            return *this;
        }
    private:
        int raw_address_;
    };
    LogicalBlockAddress operator+(const LogicalBlockAddress &a, const LogicalBlockAddress &b)
    {
        return LogicalBlockAddress(a.raw_address_ + b.raw_address_);
    }
    int operator-(const LogicalBlockAddress &a, const LogicalBlockAddress &b)
    {
        return a.raw_address_ - b.raw_address_;
    }

    class LogicalBlockRegion
    {
    public:
        LogicalBlockRegion() = delete;
        LogicalBlockRegion(const LogicalBlockAddress start, const LogicalBlockAddress end) : start_(start), end_(end)
        {
            if (start.Cmp(end).IsGreater())
            {
                abort();
            }
        }
        static bool IsOverlapped(const LogicalBlockRegion reg1, const LogicalBlockRegion reg2)
        {
            const LogicalBlockAddress s_address = LogicalBlockAddress::GetGreater(reg1.start_, reg2.start_);
            const LogicalBlockAddress e_address = LogicalBlockAddress::GetLower(reg1.end_, reg2.end_);
            return s_address.Cmp(e_address).IsLowerOrEqual();
        }
        LogicalBlockAddress GetStart() const
        {
            return start_;
        }
        LogicalBlockAddress GetEnd() const
        {
            return end_;
        }
        size_t GetRegionSize() const
        {
            return end_.GetRaw() - start_.GetRaw() + 1;
        }

    private:
        const LogicalBlockAddress start_;
        const LogicalBlockAddress end_;
    };

    // Clients read/write data from/to this buffer.
    // Each storage class can decide the way to allocate buffer.
    struct BlockBufferInterface
    {
        virtual ~BlockBufferInterface() = 0;
        virtual uint8_t *GetPtrToTheBuffer() = 0;
        static LogicalBlockAddress GetAddressFromOffset(const size_t offset)
        {
            return LogicalBlockAddress(offset / kSize);
        }
        static size_t GetInBufferOffset(const size_t offset)
        {
            return offset % kSize;
        }
        static const size_t kSize = 512;
    };
    BlockBufferInterface::~BlockBufferInterface()
    {
    }

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
        virtual uint8_t *GetPtrToTheBuffer() override
        {
            return buf_;
        }

    private:
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

    template <class BlockBuffer>
    class BlockStorageInterface
    {
    public:
        virtual ~BlockStorageInterface() = 0;
        virtual Status Open() = 0; // might be called multiple times
        Status Read(const LogicalBlockAddress address, BlockBuffer &buffer)
        {
            if (!IsValidAddress(address))
            {
                return Status::CreateErrorStatus();
            }
            return ReadInternal(address, buffer);
        }
        Status Write(const LogicalBlockAddress address, BlockBuffer &buffer)
        {
            if (!IsValidAddress(address))
            {
                return Status::CreateErrorStatus();
            }
            return WriteInternal(address, buffer);
        }
        Status ReadBlocks(const LogicalBlockRegion region, BlockBuffer *buffers)
        {
            LogicalBlockAddress address = region.GetStart();
            for (int i = 0; address.Cmp(region.GetEnd()).IsLowerOrEqual(); i++)
            {
                if (!IsValidAddress(address) || ReadInternal(address, buffers[i]).IsError())
                {
                    return Status::CreateErrorStatus();
                }
                ++address;
            }
            return Status::CreateOkStatus();
        }
        Status WriteBlocks(const LogicalBlockRegion region, BlockBuffer *buffers)
        {
            LogicalBlockAddress address = region.GetStart();
            for (int i = 0; address.Cmp(region.GetEnd()).IsLowerOrEqual(); i++)
            {
                if (!IsValidAddress(address) || WriteInternal(address, buffers[i]).IsError())
                {
                    return Status::CreateErrorStatus();
                }
                ++address;
            }
            return Status::CreateOkStatus();
        }
        virtual LogicalBlockAddress GetMaxAddress() const = 0;

    protected:
        virtual Status ReadInternal(const LogicalBlockAddress address, BlockBuffer &buffer) = 0;
        virtual Status WriteInternal(const LogicalBlockAddress address, BlockBuffer &buffer) = 0;
        bool IsValidAddress(const LogicalBlockAddress address) const
        {
            return (GetMaxAddress().Cmp(address).IsGreaterOrEqual()) && (LogicalBlockAddress(0).Cmp(address).IsLowerOrEqual());
        }

    private:
        typedef char correct_type;
        typedef struct
        {
            char dummy[2];
        } incorrect_type;

        static correct_type type_check(const volatile BlockBufferInterface *);
        static incorrect_type type_check(...);
        static_assert(sizeof(type_check((BlockBuffer *)0)) == sizeof(correct_type), "BlockBuffer should be derived from BlockBufferInterface.");
    };
    template <class BlockBuffer>
    inline BlockStorageInterface<BlockBuffer>::~BlockStorageInterface() {}
}