#pragma once
#include "block_storage_interface.h"
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

namespace HayaguiKvs
{

    class FileBlockStorage final : public BlockStorageInterface<GenericBlockBuffer>
    {
    public:
        FileBlockStorage(const char *const fname) : fname_(CopyFname(fname))
        {
        }
        virtual ~FileBlockStorage()
        {
            close(fd_);
            free(fname_);
        }
        virtual Status Open() override
        {
            if (fd_ >= 0)
            {
                return Status::CreateOkStatus();
            }
            fd_ = open(fname_, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
            if (fd_ == -1)
            {
                return Status::CreateErrorStatus();
            }
            struct stat buf;
            if (fstat(fd_, &buf) != 0)
            {
                return Status::CreateErrorStatus();
            }
            int size = buf.st_size;
            if (size < GetMaxAddress().GetRaw() * BlockBufferInterface::kSize)
            {
                ftruncate(fd_, GetMaxAddress().GetRaw() * BlockBufferInterface::kSize);
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
            if (pread(fd_, buffer.GetPtrToTheBuffer(), BlockBufferInterface::kSize, address.GetRaw() * BlockBufferInterface::kSize) != BlockBufferInterface::kSize)
            {
                return Status::CreateErrorStatus();
            }
            return Status::CreateOkStatus();
        }
        virtual Status WriteInternal(const LogicalBlockAddress address, const GenericBlockBuffer &buffer) override
        {
            if (pwrite(fd_, buffer.GetConstPtrToTheBuffer(), BlockBufferInterface::kSize, address.GetRaw() * BlockBufferInterface::kSize) != BlockBufferInterface::kSize)
            {
                return Status::CreateErrorStatus();
            }
            return Status::CreateOkStatus();
        }
        static char *const CopyFname(const char *const fname)
        {
            char *const buf = (char *)malloc(strlen(fname) + 1);
            strcpy(buf, fname);
            return buf;
        }
        static const int kNumBlocks = 4192;
        char *const fname_;
        int fd_ = -1;
    };
}