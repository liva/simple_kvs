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
        void Cmp(const LogicalBlockAddress address, CmpResult &result) const
        {
            result = CmpResult(raw_address_ - address.raw_address_);
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
    private:
        const int raw_address_;
    };

    class LogicalBlockRegion
    {
    public:
        LogicalBlockRegion() = delete;
        LogicalBlockRegion(const LogicalBlockAddress start, const LogicalBlockAddress end) : start_(start), end_(end)
        {
        }
        static bool IsOverlapped(const LogicalBlockRegion reg1, const LogicalBlockRegion reg2)
        {
            const LogicalBlockAddress s_address = LogicalBlockAddress::GetGreater(reg1.start_, reg2.start_);
            const LogicalBlockAddress e_address = LogicalBlockAddress::GetLower(reg1.end_, reg2.end_);
            CmpResult result;
            s_address.Cmp(e_address, result);
            return result.IsLowerOrEqual();
        }
        LogicalBlockAddress GetStart() const
        {
            return start_;
        }
        LogicalBlockAddress GetEnd() const
        {
            return end_;
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
        static const size_t kSize = 512;
    };
    BlockBufferInterface::~BlockBufferInterface()
    {
    }

    class GenericBlockBuffer : public BlockBufferInterface
    {
    public:
        GenericBlockBuffer() : BlockBufferInterface()
        {
            buf_ = allocator_.AllocateBuffer();
        }
        virtual uint8_t *GetPtrToTheBuffer() override
        {
            return buf_;
        }
        ~GenericBlockBuffer()
        {
            allocator_.ReleaseBuffer(buf_);
        }

    private:
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
        virtual Status Read(LogicalBlockAddress address, BlockBuffer &buffer) = 0;
        virtual Status Write(LogicalBlockAddress address, BlockBuffer &buffer) = 0;
        virtual LogicalBlockAddress GetMaxAddress() = 0;

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