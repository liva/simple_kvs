#pragma once
#include "appendonly_storage.h"
#include "utils/allocator.h"
#include "utils/slice.h"
#include "utils/optional.h"
#include <string.h>
#include <stdint.h>

namespace HayaguiKvs
{
    class UnsignedInt64ForLogInfo
    {
    public:
        UnsignedInt64ForLogInfo(const uint64_t value)
            : value_(value)
        {
        }
        ConstSlice CreateConstSlice() const
        {
            const int kBufferSize = sizeof(uint64_t);
            char buf[kBufferSize];
            memcpy(buf, &value_, kBufferSize);
            return ConstSlice(buf, kBufferSize);
        }
        static const OptionalForConstObj<UnsignedInt64ForLogInfo> ReadFromStorage(SequentialReadCharStorageInterface &char_storage)
        {
            SliceContainer container;
            if (char_storage.Read(container, 8).IsError())
            {
                return OptionalForConstObj<UnsignedInt64ForLogInfo>::CreateInvalidObj();
            }
            int len;
            if (container.GetLen(len).IsError() || len != 8)
            {
                return OptionalForConstObj<UnsignedInt64ForLogInfo>::CreateInvalidObj();
            }
            uint64_t value;
            assert(container.CopyToBuffer((char *)&value).IsOk());
            return OptionalForConstObj<UnsignedInt64ForLogInfo>::CreateValidObj(UnsignedInt64ForLogInfo(value));
        }

        const uint64_t value_;
    };
    class LogReader
    {
    public:
        LogReader(SequentialReadCharStorageInterface &char_storage)
            : char_storage_(char_storage)
        {
        }
        Status Open()
        {
            return char_storage_.Open();
        }
        Status RetrieveNextEntry(SliceContainer &container)
        {
            const OptionalForConstObj<UnsignedInt64ForLogInfo> result = UnsignedInt64ForLogInfo::ReadFromStorage(char_storage_);
            if (!result.isPresent())
            {
                return Status::CreateErrorStatus();
            }
            const UnsignedInt64ForLogInfo len = result.get();

            if (char_storage_.Read(container, len.value_).IsError())
            {
                return Status::CreateErrorStatus();
            }

            int slice_len;
            if (container.GetLen(slice_len).IsError() || slice_len != len.value_)
            {
                return Status::CreateErrorStatus();
            }
            return Status::CreateOkStatus();
        }

    private:
        SequentialReadCharStorageInterface &char_storage_;
    };

    class LogAppender
    {
    public:
        LogAppender(AppendCharStorageInterface &char_storage)
            : char_storage_(char_storage)
        {
        }
        Status Open()
        {
            return char_storage_.Open();
        }
        Status AppendEntry(const ValidSlice &obj)
        {
            UnsignedInt64ForLogInfo len(obj.GetLen());
            if (char_storage_.Append(len.CreateConstSlice()).IsError())
            {
                return Status::CreateErrorStatus();
            }
            return char_storage_.Append(obj);
        }

    private:
        AppendCharStorageInterface &char_storage_;
    };
}