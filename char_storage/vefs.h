#pragma once
#include "char_storage/interface.h"
#include "utils/buffer.h"
#include <vefs.h>

namespace HayaguiKvs
{
    class VefsCharStorage final : public AppendOnlyCharStorageInterface
    {
    public:
        VefsCharStorage(const char *const fname)
            : fname_(CopyFname(fname)), vefs_(Vefs::Get())
        {
        }
        virtual ~VefsCharStorage()
        {
            free(fname_);
        }
        virtual Status Open() override
        {
            inode_ = vefs_->Create(std::string(fname_), false);
            return Status::CreateOkStatus();
        }
        virtual Status Append(const ValidSlice &slice) override
        {
            char *buf = (char *)malloc(slice.GetLen());
            if (slice.CopyToBuffer(buf).IsError())
            {
                abort();
            }
            Vefs::Status s = vefs_->Append(inode_, buf, slice.GetLen());
            free(buf);
            if (s == Vefs::Status::kOk)
            {
                return Status::CreateOkStatus();
            }
            return Status::CreateErrorStatus();
        }
        virtual Status Append(MultipleValidSliceContainerReaderInterface &multiple_slice_container) override
        {
            const int len = multiple_slice_container.GetSliceLen();
            LocalBuffer buf(len);
            if (multiple_slice_container.CopyToBuffer(buf.GetPtr()).IsError())
            {
                abort();
            }
            Vefs::Status s = vefs_->Append(inode_, buf.GetPtr(), len);
            if (s == Vefs::Status::kOk)
            {
                return Status::CreateOkStatus();
            }
            return Status::CreateErrorStatus();
        }
        virtual Status Read(const size_t offset, const int len, SliceContainer &container) override
        {
            if (vefs_->GetLen(inode_) < offset + len) {
                return Status::CreateErrorStatus();
            }
            LocalBuffer buf(len);
            Vefs::Status s = vefs_->Read(inode_, offset, len, buf.GetPtr());
            if (s != Vefs::Status::kOk)
            {
                return Status::CreateErrorStatus();
            }
            container.Set(ConstSlice(buf.GetPtr(), len));
            return Status::CreateOkStatus();
        }
        virtual size_t GetLen() const override
        {
            return vefs_->GetLen(inode_);
        }

    private:
        static char *const CopyFname(const char *const fname)
        {
            char *const buf = (char *)malloc(strlen(fname) + 1);
            strcpy(buf, fname);
            return buf;
        }
        char *const fname_;
        Vefs *vefs_;
        Inode *inode_;
    };
}