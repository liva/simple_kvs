#pragma once
#include "block_storage/block_storage_interface.h"
#include <vefs.h>
#include <stdlib.h>
#include <stdint.h>

namespace HayaguiKvs
{
    class VefsBlockStorage final : public BlockStorageInterface<GenericBlockBuffer>
    {
    public:
        VefsBlockStorage(const char *const fname)
            : fname_(CopyFname(fname)), vefs_(Vefs::Get())
        {
        }
        virtual ~VefsBlockStorage()
        {
            if (inode_)
            {
                vefs_->SoftSync(inode_);
            }
            free(fname_);
        }
        virtual Status Open() override
        {
            if (!inode_)
            {
                inode_ = vefs_->Create(std::string(fname_), false);
            }
            return Status::CreateOkStatus();
        }
        virtual LogicalBlockAddress GetMaxAddress() const override
        {
            return LogicalBlockAddress(kNumBlocks - 1);
        }

    private:
        virtual Status ReadInternal(const LogicalBlockAddress address, GenericBlockBuffer &buffer) override
        {
            uint8_t *buf = (uint8_t *)malloc(BlockBufferInterface::kSize);
            Vefs::Status s = vefs_->Read(inode_, address.GetRaw() * BlockBufferInterface::kSize, BlockBufferInterface::kSize, (char *)buf);
            buffer.CopyFrom(buf, 0, BlockBufferInterface::kSize);
            free(buf);
            if (s == Vefs::Status::kOk)
            {
                return Status::CreateOkStatus();
            }
            return Status::CreateErrorStatus();
        }
        virtual Status WriteInternal(const LogicalBlockAddress address, const GenericBlockBuffer &buffer) override
        {
            uint8_t *buf = (uint8_t *)malloc(BlockBufferInterface::kSize);
            buffer.CopyTo(buf, 0, BlockBufferInterface::kSize);
            Vefs::Status s = vefs_->Write(inode_, address.GetRaw() * BlockBufferInterface::kSize, buf, HayaguiKvs::BlockBufferInterface::kSize);
            free(buf);
            if (s == Vefs::Status::kOk)
            {
                return Status::CreateOkStatus();
            }
            return Status::CreateErrorStatus();
        }
        static char *const CopyFname(const char *const fname)
        {
            char *const buf = (char *)malloc(strlen(fname) + 1);
            strcpy(buf, fname);
            return buf;
        }
        static const int kNumBlocks = 4192;
        char *const fname_;
        Vefs *vefs_;
        Inode *inode_ = nullptr;
    };
}
