#include "appendonly_storage.h"
#include "block_storage/memblock_storage.h"
#include "./test.h"
#include "../utils/debug.h"
#include <memory>
#include <vector>
std::vector<int> dummy;

using namespace HayaguiKvs;

static ConstSlice CreateSliceFromChar(char c, int cnt)
{
    char buf[cnt];
    for (int i = 0; i < cnt; i++)
    {
        buf[i] = c;
    }
    return ConstSlice(buf, cnt);
}

static void append_only_storage()
{
    START_TEST;
    MemBlockStorage block_storage;
    AppendOnlyCharStorage<GenericBlockBuffer> append_only_storage(block_storage);
    assert(append_only_storage.Open().IsOk());
    for (char c = 'A'; c <= 'Z'; c++)
    {
        int cnt = 'Z' - c;
        ConstSlice slice = CreateSliceFromChar(c, cnt);
        size_t prev_len = append_only_storage.GetLen();
        assert(append_only_storage.Append(slice).IsOk());
        assert(append_only_storage.GetLen() == prev_len + cnt);
    }
    SequentialReadStorageOverRandomReadStorage sequential_read_storage(append_only_storage);
    for (char c = 'A'; c <= 'Z'; c++)
    {
        int cnt = 'Z' - c;
        SliceContainer container;
        assert(sequential_read_storage.Open().IsOk());
        assert(sequential_read_storage.Read(container, cnt).IsOk());
        int read_len;
        assert(container.GetLen(read_len).IsOk());
        assert(read_len == cnt);
        ConstSlice slice = CreateSliceFromChar(c, cnt);
        assert(container.DoesMatch(slice));
    }
}

int main()
{
    append_only_storage();
    return 0;
}