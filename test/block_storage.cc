#include "block_storage/block_storage_interface.h"
#include "block_storage/memblock_storage.h"
#include "block_storage/block_storage_multiplier.h"
#include "block_storage/block_storage_with_cache.h"
#include "./test.h"
#include <memory>
#include <vector>
std::vector<int> dummy;

using namespace HayaguiKvs;

static void cmp_lba()
{
    START_TEST;
    LogicalBlockAddress address1(0);
    LogicalBlockAddress address2(8);
    LogicalBlockAddress address3(20);
    assert(address1.Cmp(address1).IsEqual());
    assert(address1.Cmp(address2).IsLower());
    assert(address3.Cmp(address2).IsGreater());
}

static void check_region_overlapped()
{
    START_TEST;
    {
        LogicalBlockRegion reg1(LogicalBlockAddress(0), LogicalBlockAddress(5));
        LogicalBlockRegion reg2(LogicalBlockAddress(3), LogicalBlockAddress(8));
        assert(LogicalBlockRegion::IsOverlapped(reg1, reg2));
        assert(LogicalBlockRegion::IsOverlapped(reg2, reg1));
    }
    {
        LogicalBlockRegion reg1(LogicalBlockAddress(0), LogicalBlockAddress(3));
        LogicalBlockRegion reg2(LogicalBlockAddress(3), LogicalBlockAddress(8));
        assert(LogicalBlockRegion::IsOverlapped(reg1, reg2));
        assert(LogicalBlockRegion::IsOverlapped(reg2, reg1));
    }
    {
        LogicalBlockRegion reg1(LogicalBlockAddress(0), LogicalBlockAddress(8));
        LogicalBlockRegion reg2(LogicalBlockAddress(3), LogicalBlockAddress(5));
        assert(LogicalBlockRegion::IsOverlapped(reg1, reg2));
        assert(LogicalBlockRegion::IsOverlapped(reg2, reg1));
    }
    {
        LogicalBlockRegion reg1(LogicalBlockAddress(0), LogicalBlockAddress(8));
        LogicalBlockRegion reg2(LogicalBlockAddress(3), LogicalBlockAddress(8));
        assert(LogicalBlockRegion::IsOverlapped(reg1, reg2));
        assert(LogicalBlockRegion::IsOverlapped(reg2, reg1));
    }
    {
        LogicalBlockRegion reg1(LogicalBlockAddress(0), LogicalBlockAddress(3));
        LogicalBlockRegion reg2(LogicalBlockAddress(5), LogicalBlockAddress(8));
        assert(!LogicalBlockRegion::IsOverlapped(reg1, reg2));
        assert(!LogicalBlockRegion::IsOverlapped(reg2, reg1));
    }
}

static void InitializeBuffer(BlockBufferInterface &buf, int starting_number)
{
    for (int i = 0; i < BlockBufferInterface::kSize; i++)
    {
        buf.SetValue<uint8_t>(i, (starting_number * 100 + i) % 0xFF);
    }
}

static Status CheckBuffer(BlockBufferInterface &buf, int starting_number)
{
    for (int i = 0; i < BlockBufferInterface::kSize; i++)
    {
        if (buf.GetValue<uint8_t>(i) != (starting_number * 100 + i) % 0xFF)
        {
            return Status::CreateErrorStatus();
        }
    }
    return Status::CreateOkStatus();
}

class BuffersManager
{
public:
    BuffersManager() : buffers_(kBufferCnt) {}
    void InitializeBuffers(int starting_number)
    {
        for (int i = 0; i < kBufferCnt; i++)
        {
            InitializeBuffer(*buffers_.GetBlockBufferFromIndex(i), (i + 1) * 20 + starting_number);
        }
    }
    Status CheckBuffers(int starting_number)
    {
        for (int i = 0; i < kBufferCnt; i++)
        {
            if (CheckBuffer(*buffers_.GetBlockBufferFromIndex(i), (i + 1) * 20 + starting_number).IsError())
            {
                return Status::CreateErrorStatus();
            }
        }
        return Status::CreateOkStatus();
    }
    BlockBuffers<GenericBlockBuffer> &GetRefOfBuffers()
    {
        return buffers_;
    }
    static const int kBufferCnt = 5;

private:
    BlockBuffers<GenericBlockBuffer> buffers_;
};

static void memblock_storage()
{
    START_TEST;
    std::unique_ptr<BlockStorageInterface<GenericBlockBuffer>> storage = std::unique_ptr<MemBlockStorage>(new MemBlockStorage);
    assert(storage->Open().IsOk());
    GenericBlockBuffer buf1, buf2, buf3;
    InitializeBuffer(buf1, 0);
    InitializeBuffer(buf2, 10);
    InitializeBuffer(buf3, 20);
    assert(storage->Write(LogicalBlockAddress(0), buf1).IsOk());
    assert(storage->Write(storage->GetMaxAddress(), buf3).IsOk());
    assert(storage->Write(LogicalBlockAddress(0), buf2).IsOk());

    GenericBlockBuffer buf4, buf5;
    assert(storage->Read(LogicalBlockAddress(0), buf4).IsOk());
    assert(storage->Read(storage->GetMaxAddress(), buf5).IsOk());

    assert(CheckBuffer(buf4, 10).IsOk());
    assert(CheckBuffer(buf5, 20).IsOk());

    BuffersManager buffers_manager;
    buffers_manager.InitializeBuffers(30);
    assert(storage->WriteBlocks(LogicalBlockRegion(LogicalBlockAddress(0), LogicalBlockAddress(buffers_manager.kBufferCnt - 1)), buffers_manager.GetRefOfBuffers()).IsOk());
    buffers_manager.InitializeBuffers(80);
    assert(storage->ReadBlocks(LogicalBlockRegion(LogicalBlockAddress(0), LogicalBlockAddress(buffers_manager.kBufferCnt - 1)), buffers_manager.GetRefOfBuffers()).IsOk());
    assert(buffers_manager.CheckBuffers(30).IsOk());
}

static void multiplier()
{
    START_TEST;
    MultiplyRule rule;
    int i;
    assert(rule.AppendRegion(LogicalBlockRegion(LogicalBlockAddress(0), LogicalBlockAddress(5)), i).IsOk());
    assert(i == 0);
    assert(rule.AppendRegion(LogicalBlockRegion(LogicalBlockAddress(7), LogicalBlockAddress(10)), i).IsOk());
    assert(i == 1);
    std::shared_ptr<BlockStorageInterface<GenericBlockBuffer>> storage = std::shared_ptr<MemBlockStorage>(new MemBlockStorage);
    assert(storage->Open().IsOk());
    {
        GenericBlockBuffer buf1, buf2, buf3;
        InitializeBuffer(buf1, 0);
        InitializeBuffer(buf2, 10);
        InitializeBuffer(buf3, 20);
        assert(storage->Write(LogicalBlockAddress(2), buf1).IsOk());
        assert(storage->Write(LogicalBlockAddress(5), buf2).IsOk());
        assert(storage->Write(LogicalBlockAddress(9), buf3).IsOk());
    }

    std::shared_ptr<BlockStorageMultiplier<GenericBlockBuffer>> multiplier = std::shared_ptr<BlockStorageMultiplier<GenericBlockBuffer>>(new BlockStorageMultiplier<GenericBlockBuffer>(*storage, rule));
    {
        GenericBlockBuffer buf1;
        BlockStorageMultiplier<GenericBlockBuffer>::MultipliedBlockStorage storage1 = multiplier->GetMultipliedBlockStorage(0);
        BlockStorageMultiplier<GenericBlockBuffer>::MultipliedBlockStorage storage2 = multiplier->GetMultipliedBlockStorage(1);
        assert(storage1.Open().IsOk());
        assert(storage2.Open().IsOk());
        assert(storage1.Read(LogicalBlockAddress(2), buf1).IsOk());
        assert(CheckBuffer(buf1, 0).IsOk());
        assert(storage1.Read(LogicalBlockAddress(5), buf1).IsOk());
        assert(CheckBuffer(buf1, 10).IsOk());
        assert(storage1.Read(LogicalBlockAddress(8), buf1).IsError());
        assert(storage2.Read(LogicalBlockAddress(2), buf1).IsOk());
        assert(CheckBuffer(buf1, 20).IsOk());
        assert(storage2.Read(LogicalBlockAddress(4), buf1).IsError());
        InitializeBuffer(buf1, 30);
        assert(storage2.Write(LogicalBlockAddress(3), buf1).IsOk());
    }
    {
        GenericBlockBuffer buf1;
        assert(storage->Read(LogicalBlockAddress(10), buf1).IsOk());
        assert(CheckBuffer(buf1, 30).IsOk());
    }
}

template <class BlockBuffer>
class TestStorage : public BlockStorageInterface<BlockBuffer>
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
    bool IsReadCntIncremented()
    {
        bool flag = read_cnt_ == last_read_cnt_ + 1;
        last_read_cnt_ = read_cnt_;
        return flag;
    }
    bool IsWriteCntIncremented()
    {
        bool flag = write_cnt_ == last_write_cnt_ + 1;
        last_write_cnt_ = write_cnt_;
        return flag;
    }
    virtual LogicalBlockAddress GetMaxAddress() const override
    {
        return storage_.GetMaxAddress();
    }
    Status ReadUnderlyingStorage(const LogicalBlockAddress address, BlockBuffer &buffer)
    {
        return storage_.Read(address, buffer);
    }
    Status WriteUnderlyingStorage(const LogicalBlockAddress address, const BlockBuffer &buffer)
    {
        return storage_.Write(address, buffer);
    }

private:
    virtual Status ReadInternal(const LogicalBlockAddress address, BlockBuffer &buffer) override
    {
        read_cnt_++;
        return storage_.Read(address, buffer);
    }
    virtual Status WriteInternal(const LogicalBlockAddress address, const BlockBuffer &buffer) override
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

class Checker
{
public:
    Checker() = delete;
    Checker(BlockStorageWithOneCache<GenericBlockBuffer> &storage_with_cache, TestStorage<GenericBlockBuffer> &underlying_storage)
        : storage_with_cache_(storage_with_cache),
          underlying_storage_(underlying_storage)
    {
    }
    void Write(const LogicalBlockAddress address, const int signature)
    {
        GenericBlockBuffer buf;
        buf.SetValue<int>(8, signature);
        assert(storage_with_cache_.Write(address, buf).IsOk());
        assert(underlying_storage_.IsWriteCntIncremented());
    }
    Status ReadFromCache(const LogicalBlockAddress address, const int signature)
    {
        GenericBlockBuffer buf;
        buf.SetValue<int>(8, 0); // to ensure the value is actually stored
        if (storage_with_cache_.Read(address, buf).IsError())
        {
            return Status::CreateErrorStatus();
        }
        if (underlying_storage_.IsReadCntIncremented())
        {
            return Status::CreateErrorStatus();
        }
        if (buf.GetValue<int>(8) != signature)
        {
            return Status::CreateErrorStatus();
        }
        return Status::CreateOkStatus();
    }
    Status ReadFromStorage(const LogicalBlockAddress address, const int signature)
    {
        GenericBlockBuffer buf;
        buf.SetValue<int>(8, 0); // to ensure the value is actually stored
        if (storage_with_cache_.Read(address, buf).IsError())
        {
            return Status::CreateErrorStatus();
        }
        if (!underlying_storage_.IsReadCntIncremented())
        {
            return Status::CreateErrorStatus();
        }
        if (buf.GetValue<int>(8) != signature)
        {
            return Status::CreateErrorStatus();
        }
        return Status::CreateOkStatus();
    }

private:
    BlockStorageWithOneCache<GenericBlockBuffer> &storage_with_cache_;
    TestStorage<GenericBlockBuffer> &underlying_storage_;
};

static void cache()
{
    START_TEST;
    TestStorage<GenericBlockBuffer> underlying_storage;
    BlockStorageWithOneCache<GenericBlockBuffer> storage_with_cache(underlying_storage);
    Checker checker(storage_with_cache, underlying_storage);
    GenericBlockBuffer buf;
    assert(storage_with_cache.Open().IsOk());

    underlying_storage.ResetCnt();

    // check cache is not used before setting it
    static int kSignature1 = 123;
    assert(storage_with_cache.Read(LogicalBlockAddress(0), buf).IsOk());
    assert(underlying_storage.IsReadCntIncremented());
    checker.Write(LogicalBlockAddress(0), kSignature1);
    assert(checker.ReadFromStorage(LogicalBlockAddress(0), kSignature1).IsOk());

    // check cache is used
    assert(storage_with_cache.SetCacheAddress(LogicalBlockAddress(0)).IsOk());
    assert(checker.ReadFromStorage(LogicalBlockAddress(0), kSignature1).IsOk());
    assert(checker.ReadFromCache(LogicalBlockAddress(0), kSignature1).IsOk());

    // check if cache is applied to both buffer and underlying storage at write
    checker.Write(LogicalBlockAddress(0), kSignature1);
    buf.SetValue<int>(8, 0); // to ensure the value is actually stored
    assert(underlying_storage.ReadUnderlyingStorage(LogicalBlockAddress(0), buf).IsOk());
    assert(buf.GetValue<int>(8) == kSignature1);
    assert(checker.ReadFromCache(LogicalBlockAddress(0), kSignature1).IsOk());

    // check if the storage returns correct block even if its cache is not yet obtained.
    static int kSignature2 = 456;
    checker.Write(LogicalBlockAddress(1), kSignature2);
    assert(storage_with_cache.SetCacheAddress(LogicalBlockAddress(1)).IsOk());
    assert(checker.ReadFromCache(LogicalBlockAddress(0), kSignature1).IsOk()); // cache will still be available and used
    assert(checker.ReadFromStorage(LogicalBlockAddress(1), kSignature2).IsOk());
    assert(checker.ReadFromCache(LogicalBlockAddress(1), kSignature2).IsOk());

    // check if write to the cache target block is cached
    static int kSignature3 = 456;
    assert(storage_with_cache.SetCacheAddress(LogicalBlockAddress(2)).IsOk());
    checker.Write(LogicalBlockAddress(2), kSignature3);
    assert(checker.ReadFromCache(LogicalBlockAddress(2), kSignature3).IsOk());
}

int main()
{
    cmp_lba();
    memblock_storage();
    check_region_overlapped();
    multiplier();
    cache();
    return 0;
}