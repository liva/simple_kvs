#pragma once
#include "block_storage/block_storage_multiplier.h"
#include "block_storage/block_storage_with_cache.h"
#include "utils/slice.h"
#include <assert.h>
#include <string.h>

namespace HayaguiKvs
{
    struct RandomReadCharStorageInterface
    {
        virtual ~RandomReadCharStorageInterface() = 0;
        virtual Status Open() = 0;
        virtual Status Read(const size_t offset, const int len, SliceContainer &container) = 0;
        virtual size_t GetLen() const = 0;
    };
    inline RandomReadCharStorageInterface::~RandomReadCharStorageInterface() {}

    struct SequentialReadCharStorageInterface
    {
        virtual ~SequentialReadCharStorageInterface() = 0;
        virtual Status Open() = 0;
        virtual void SeekTo(const size_t offset) = 0;
        virtual Status Read(SliceContainer &container, const int len) = 0;
        virtual size_t GetLen() const = 0;
    };
    inline SequentialReadCharStorageInterface::~SequentialReadCharStorageInterface() {}

    class SequentialReadCharStorageOverRandomReadCharStorage : public SequentialReadCharStorageInterface
    {
    public:
        SequentialReadCharStorageOverRandomReadCharStorage(RandomReadCharStorageInterface &underlying_storage) : underlying_storage_(underlying_storage)
        {
        }
        virtual Status Open() override
        {
            if (underlying_storage_.Open().IsError())
            {
                return Status::CreateErrorStatus();
            }
            return Status::CreateOkStatus();
        }
        virtual void SeekTo(const size_t offset) override
        {
            offset_ = offset;
        }
        virtual Status Read(SliceContainer &container, const int len) override
        {
            if (underlying_storage_.Read(offset_, len, container).IsError())
            {
                return Status::CreateErrorStatus();
            }
            int slice_len;
            Status s1 = container.GetLen(slice_len);
            assert(s1.IsOk());
            offset_ += slice_len;
            return Status::CreateOkStatus();
        }
        virtual size_t GetLen() const override
        {
            return underlying_storage_.GetLen();
        }

    private:
        size_t offset_ = 0;
        RandomReadCharStorageInterface &underlying_storage_;
    };


    struct AppendCharStorageInterface
    {
        virtual ~AppendCharStorageInterface() = 0;
        virtual Status Open() = 0;
        virtual Status Append(const ValidSlice &slice) = 0;
        virtual size_t GetLen() const = 0;
    };
    AppendCharStorageInterface::~AppendCharStorageInterface() {}

    struct AppendOnlyCharStorageInterface : public RandomReadCharStorageInterface, public AppendCharStorageInterface
    {
        virtual Status Open() override = 0;
        virtual size_t GetLen() const override = 0;
    };

}