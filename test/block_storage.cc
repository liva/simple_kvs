#include "block_storage/block_storage_interface.h"
#include "block_storage/memblock_storage.h"
#include "block_storage/block_storage_multiplier.h"
#include "./test.h"
#include "../utils/debug.h"
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

static void InitializeBuffer(GenericBlockBuffer &buf, int starting_number)
{
    for (int i = 0; i < BlockBufferInterface::kSize; i++)
    {
        buf.GetPtrToTheBuffer()[i] = (starting_number * 100 + i) % 0xFF;
    }
}

static Status CheckBuffer(GenericBlockBuffer &buf, int starting_number)
{
    for (int i = 0; i < BlockBufferInterface::kSize; i++)
    {
        if (buf.GetPtrToTheBuffer()[i] != (starting_number * 100 + i) % 0xFF)
        {
            return Status::CreateErrorStatus();
        }
    }
    return Status::CreateOkStatus();
}

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

int main()
{
    cmp_lba();
    memblock_storage();
    check_region_overlapped();
    multiplier();
    return 0;
}