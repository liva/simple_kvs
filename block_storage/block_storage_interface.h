#pragma once
#include "utils/status.h"
#include "lba.h"
#include "buffer.h"

namespace HayaguiKvs
{
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
        Status Write(const LogicalBlockAddress address, const BlockBuffer &buffer)
        {
            if (!IsValidAddress(address))
            {
                return Status::CreateErrorStatus();
            }
            return WriteInternal(address, buffer);
        }
        Status ReadBlocks(const LogicalBlockRegion region, BlockBuffers<BlockBuffer> &buffers)
        {
            LogicalBlockAddress address = region.GetStart();
            for (int i = 0; address.Cmp(region.GetEnd()).IsLowerOrEqual(); i++)
            {
                if (!IsValidAddress(address) || ReadInternal(address, *buffers.GetBlockBufferFromIndex(i)).IsError())
                {
                    return Status::CreateErrorStatus();
                }
                ++address;
            }
            return Status::CreateOkStatus();
        }
        Status WriteBlocks(const LogicalBlockRegion region, const BlockBuffers<BlockBuffer> &buffers)
        {
            LogicalBlockAddress address = region.GetStart();
            for (int i = 0; address.Cmp(region.GetEnd()).IsLowerOrEqual(); i++)
            {
                if (!IsValidAddress(address) || WriteInternal(address, *buffers.GetConstBlockBufferFromIndex(i)).IsError())
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
        virtual Status WriteInternal(const LogicalBlockAddress address, const BlockBuffer &buffer) = 0;
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