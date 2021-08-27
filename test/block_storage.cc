#include "../block_storage_interface.h"
#include "../memblock_storage.h"
#include "../block_storage_multiplier.h"
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
    CmpResult result;
    address1.Cmp(address1, result);
    assert(result.IsEqual());
    address1.Cmp(address2, result);
    assert(result.IsLower());
    address3.Cmp(address2, result);
    assert(result.IsGreater());
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

static void CheckBuffer(GenericBlockBuffer &buf, int starting_number)
{
    for (int i = 0; i < BlockBufferInterface::kSize; i++)
    {
        assert(buf.GetPtrToTheBuffer()[i] == (starting_number * 100 + i) % 0xFF);
    }
}

static void memblock_storage()
{
    START_TEST;
    std::unique_ptr<BlockStorageInterface<GenericBlockBuffer>> storage = std::unique_ptr<MemBlockStorage>(new MemBlockStorage);
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

    CheckBuffer(buf4, 10);
    CheckBuffer(buf5, 20);
}

static void multiplier()
{
    START_TEST;
    std::shared_ptr<BlockStorageInterface<GenericBlockBuffer>> storage = std::shared_ptr<MemBlockStorage>(new MemBlockStorage);
    std::shared_ptr<BlockStorageMultiplier<GenericBlockBuffer>> multiplier = std::shared_ptr<BlockStorageMultiplier<GenericBlockBuffer>>(new BlockStorageMultiplier<GenericBlockBuffer>(storage));
}

int main()
{
    cmp_lba();
    memblock_storage();
    check_region_overlapped();
    multiplier();
    return 0;
}