#pragma once
#include "char_storage/interface.h"
#include "utils/allocator.h"
#include "utils/slice.h"
#include "utils/optional.h"
#include "utils/multipleslice_container.h"
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
            Status s1 = container.CopyToBuffer((char *)&value);
            assert(s1.IsOk());
            return OptionalForConstObj<UnsignedInt64ForLogInfo>::CreateValidObj(UnsignedInt64ForLogInfo(value));
        }

        const uint64_t value_;
    };
    class UnsignedInt64ForLogInfoSliceContainer
    {
    public:
        UnsignedInt64ForLogInfoSliceContainer(const uint64_t value)
            : slice_(buf_, sizeof(uint64_t))
        {
            *(uint64_t *)buf_ = value;
        }
        ValidSlice &GetSlice()
        {
            return slice_;
        }

    private:
        char buf_[sizeof(uint64_t)];
        BufferPtrSlice slice_;
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
        Status AppendEntries(MultipleValidSliceContainerReaderInterface &entries)
        {
            const ValidSlice *slices[entries.GetLen() * 2];
            MultipleValidSliceContainer written_slices(slices, entries.GetLen() * 2);
            return AppendEntriesSub(written_slices, entries);
        }
        Status AppendEntry(const ValidSlice &obj)
        {
            UnsignedInt64ForLogInfoSliceContainer len(obj.GetLen());
            if (char_storage_.Append(len.GetSlice()).IsError())
            {
                return Status::CreateErrorStatus();
            }
            return char_storage_.Append(obj);
        }

    private:
        Status AppendEntriesSub(MultipleValidSliceContainer &written_slices, MultipleValidSliceContainerReaderInterface &entries)
        {
            const ValidSlice *slice = entries.Get();
            UnsignedInt64ForLogInfoSliceContainer len(slice->GetLen());
            written_slices.Set(&len.GetSlice());
            written_slices.Set(slice);
            if (written_slices.GetLen() == entries.GetLen() * 2)
            {
                return char_storage_.Append(written_slices);
            }
            else
            {
                return AppendEntriesSub(written_slices, entries);
            }
        }
        AppendCharStorageInterface &char_storage_;
    };
}