#pragma once
#include "char_storage/interface.h"
#include <vefs.h>
#include <utils/debug.h>

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
        virtual Status Open()
        {
            inode_ = vefs_->Create(std::string(fname_), false);
            return Status::CreateOkStatus();
        }
        virtual Status Append(const ValidSlice &slice)
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
        virtual Status Read(const size_t offset, const int len, SliceContainer &container)
        {
            char *buf = (char *)malloc(len);
            Vefs::Status s = vefs_->Read(inode_, offset, len, buf);
            if (s != Vefs::Status::kOk)
            {
                free(buf);
                return Status::CreateErrorStatus();
            }
            container.Set(ConstSlice(buf, len));
            free(buf);
            return Status::CreateOkStatus();
        }
        virtual size_t GetLen() const
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