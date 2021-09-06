#pragma once
#include "block_storage/block_storage_interface.h"
#include "block_storage/memblock_storage.h"

using namespace HayaguiKvs;
class TestStorage : public BlockStorageInterface<GenericBlockBuffer>
{
public:
    TestStorage()
    {
    }
    virtual ~TestStorage()
    {
    }

    virtual Status Open() override
    {
        return Status::CreateOkStatus();
    }
    void ResetCnt()
    {
        last_read_cnt_ = 0;
        last_write_cnt_ = 0;
        read_cnt_ = 0;
        write_cnt_ = 0;
    }
    bool IsReadCntAdded(const int cnt)
    {
        bool flag = read_cnt_ == last_read_cnt_ + cnt;
        last_read_cnt_ = read_cnt_;
        return flag;
    }
    bool IsWriteCntAdded(const int cnt)
    {
        bool flag = write_cnt_ == last_write_cnt_ + cnt;
        last_write_cnt_ = write_cnt_;
        return flag;
    }
    bool IsReadCntIncremented()
    {
        return IsReadCntAdded(1);
    }
    bool IsWriteCntIncremented()
    {
        return IsWriteCntAdded(1);
    }
    virtual LogicalBlockAddress GetMaxAddress() const override
    {
        return storage_.GetMaxAddress();
    }
    Status ReadUnderlyingStorage(const LogicalBlockAddress address, GenericBlockBuffer &buffer)
    {
        return storage_.Read(address, buffer);
    }
    Status WriteUnderlyingStorage(const LogicalBlockAddress address, const GenericBlockBuffer &buffer)
    {
        return storage_.Write(address, buffer);
    }

private:
    virtual Status ReadInternal(const LogicalBlockAddress address, GenericBlockBuffer &buffer) override
    {
        read_cnt_++;
        return storage_.Read(address, buffer);
    }
    virtual Status WriteInternal(const LogicalBlockAddress address, const GenericBlockBuffer &buffer) override
    {
        write_cnt_++;
        return storage_.Write(address, buffer);
    }
    int last_read_cnt_ = 0;
    int last_write_cnt_ = 0;
    int read_cnt_ = 0;
    int write_cnt_ = 0;
    MemBlockStorage storage_;
};