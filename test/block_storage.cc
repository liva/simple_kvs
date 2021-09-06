#include "block_storage/block_storage_interface.h"
#include "block_storage/memblock_storage.h"
#include "block_storage/file_block_storage.h"
#include "block_storage/block_storage_multiplier.h"
#include "block_storage/block_storage_with_cache.h"
#include "./test.h"
#include "test_storage.h"
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

class BlockStorageTester
{
public:
    BlockStorageTester(BlockStorageInterface<GenericBlockBuffer> &storage) : storage_(storage)
    {
    }
    void Open()
    {
        assert(storage_.Open().IsOk());
    }
    void Write()
    {
        GenericBlockBuffer buf1, buf2, buf3;
        InitializeBuffer(buf1, 0);
        InitializeBuffer(buf2, 10);
        InitializeBuffer(buf3, 20);
        assert(storage_.Write(LogicalBlockAddress(0), buf1).IsOk());
        assert(storage_.Write(storage_.GetMaxAddress(), buf3).IsOk());
        assert(storage_.Write(LogicalBlockAddress(0), buf2).IsOk());

        BuffersManager buffers_manager;
        buffers_manager.InitializeBuffers(30);
        assert(storage_.WriteBlocks(LogicalBlockRegion(LogicalBlockAddress(1), LogicalBlockAddress(buffers_manager.kBufferCnt)), buffers_manager.GetRefOfBuffers()).IsOk());
    }
    void Read()
    {
        GenericBlockBuffer buf4, buf5;
        assert(storage_.Read(LogicalBlockAddress(0), buf4).IsOk());
        assert(storage_.Read(storage_.GetMaxAddress(), buf5).IsOk());

        assert(CheckBuffer(buf4, 10).IsOk());
        assert(CheckBuffer(buf5, 20).IsOk());

        BuffersManager buffers_manager;
        buffers_manager.InitializeBuffers(80);
        assert(storage_.ReadBlocks(LogicalBlockRegion(LogicalBlockAddress(1), LogicalBlockAddress(buffers_manager.kBufferCnt)), buffers_manager.GetRefOfBuffers()).IsOk());
        assert(buffers_manager.CheckBuffers(30).IsOk());
    }

private:
    BlockStorageInterface<GenericBlockBuffer> &storage_;
};

static void memblock_storage()
{
    START_TEST;
    MemBlockStorage storage;
    BlockStorageTester tester(storage);
    tester.Open();
    tester.Write();
    tester.Read();
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

class Checker
{
public:
    Checker() = delete;
    Checker(BlockStorageWithOneCache<GenericBlockBuffer> &storage_with_cache, TestStorage &underlying_storage)
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
    TestStorage &underlying_storage_;
};

static void cache()
{
    START_TEST;
    TestStorage underlying_storage;
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

class File
{
public:
    void Init()
    {
        if (access(fname_, F_OK) == 0)
        {
            remove(fname_);
        }
    }
    void Cleanup()
    {
        remove(fname_);
    }
    static constexpr const char *const fname_ = "storage_file";
};

static void file_block_storage()
{
    START_TEST;
    File file;
    file.Init();
    {
        FileBlockStorage storage(file.fname_);
        BlockStorageTester tester(storage);
        tester.Open();
        tester.Write();
    }
    {
        FileBlockStorage storage(file.fname_);
        BlockStorageTester tester(storage);
        tester.Open();
        tester.Read();
    }
    file.Cleanup();
}

int main()
{
    cmp_lba();
    memblock_storage();
    check_region_overlapped();
    multiplier();
    cache();
    file_block_storage();
    return 0;
}