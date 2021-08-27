#pragma once
#include "block_storage_interface.h"
#include <assert.h>
#include <stdint.h>
#include <string.h>

namespace HayaguiKvs
{
    class MemBlockStorage : public BlockStorageInterface<GenericBlockBuffer>
    {
    public:
        MemBlockStorage()
        {
            for (int i = 0; i < kNumBlocks; i++)
            {
                block_buf_[i] = new MemInternalBlock;
            }
        }
        virtual ~MemBlockStorage()
        {
            for (int i = 0; i < kNumBlocks; i++)
            {
                delete block_buf_[i];
            }
        }
        virtual Status Read(LogicalBlockAddress address, GenericBlockBuffer &buffer) override
        {
            if (!IsValidAddress(address))
            {
                return Status::CreateErrorStatus();
            }
            return GetBlockFromAddress(address)->Read(buffer);
        }
        virtual Status Write(LogicalBlockAddress address, GenericBlockBuffer &buffer) override
        {
            if (!IsValidAddress(address))
            {
                return Status::CreateErrorStatus();
            }
            return GetBlockFromAddress(address)->Write(buffer);
        }
        virtual LogicalBlockAddress GetMaxAddress() override
        {
            return LogicalBlockAddress(kNumBlocks - 1);
        }
    private:
        class MemInternalBlock
        {
        public:
            Status Read(GenericBlockBuffer &buffer)
            {
                memcpy(buffer.GetPtrToTheBuffer(), buf, BlockBufferInterface::kSize);
                return Status::CreateOkStatus();
            }
            Status Write(GenericBlockBuffer &buffer)
            {
                memcpy(buf, buffer.GetPtrToTheBuffer(), BlockBufferInterface::kSize);
                return Status::CreateOkStatus();
            }
        private:
            uint8_t buf[BlockBufferInterface::kSize];
        };
        bool IsValidAddress(LogicalBlockAddress address)
        {
            CmpResult maxcheck_result, positivecheck_result;
            GetMaxAddress().Cmp(address, maxcheck_result);
            LogicalBlockAddress(0).Cmp(address, positivecheck_result);
            return (maxcheck_result.IsGreaterOrEqual()) && (positivecheck_result.IsLowerOrEqual());
        }
        MemInternalBlock *GetBlockFromAddress(LogicalBlockAddress address)
        {
            return block_buf_[address.GetRaw()];
        }
        static const int kNumBlocks = 1024;
        MemInternalBlock *block_buf_[kNumBlocks];
    };
}