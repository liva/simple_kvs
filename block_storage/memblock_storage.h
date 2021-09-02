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
        virtual Status Open() override
        {
            return Status::CreateOkStatus();
        }
        virtual LogicalBlockAddress GetMaxAddress() const override
        {
            return LogicalBlockAddress(kNumBlocks - 1);
        }

    private:
        class MemInternalBlock
        {
        public:
            Status Read(GenericBlockBuffer &buffer) const
            {
                buffer.CopyFrom(buf_, 0, BlockBufferInterface::kSize);
                return Status::CreateOkStatus();
            }
            Status Write(const GenericBlockBuffer &buffer)
            {
                buffer.CopyTo(buf_, 0, BlockBufferInterface::kSize);
                return Status::CreateOkStatus();
            }

        private:
            uint8_t buf_[BlockBufferInterface::kSize];
        };
        virtual Status ReadInternal(const LogicalBlockAddress address, GenericBlockBuffer &buffer) override
        {
            return GetBlockFromAddress(address)->Read(buffer);
        }
        virtual Status WriteInternal(const LogicalBlockAddress address, const GenericBlockBuffer &buffer) override
        {
            return GetBlockFromAddress(address)->Write(buffer);
        }
        MemInternalBlock *GetBlockFromAddress(const LogicalBlockAddress address)
        {
            return block_buf_[address.GetRaw()];
        }
        static const int kNumBlocks = 1024;
        MemInternalBlock *block_buf_[kNumBlocks];
    };
}