#include "appendonly_storage.h"
#include "block_storage/memblock_storage.h"
#include "log.h"
#include "./test.h"
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
    {
        AppendOnlyCharStorage<GenericBlockBuffer> append_only_storage(block_storage);
        assert(append_only_storage.Open().IsOk());
        for (char c = 'A'; c <= 'Z'; c++)
        {
            int cnt = ('Z' - c) * 10;
            ConstSlice slice = CreateSliceFromChar(c, cnt);
            size_t prev_len = append_only_storage.GetLen();
            assert(append_only_storage.Append(slice).IsOk());
            assert(append_only_storage.GetLen() == prev_len + cnt);
        }
        SequentialReadCharStorageOverRandomReadCharStorage sequential_read_storage(append_only_storage);
        assert(sequential_read_storage.Open().IsOk());
        for (char c = 'A'; c <= 'Z'; c++)
        {
            int cnt = ('Z' - c) * 10;
            SliceContainer container;
            assert(sequential_read_storage.Read(container, cnt).IsOk());
            int read_len;
            assert(container.GetLen(read_len).IsOk());
            assert(read_len == cnt);
            ConstSlice slice = CreateSliceFromChar(c, cnt);
            assert(container.DoesMatch(slice));
        }
    }
    {
        AppendOnlyCharStorage<GenericBlockBuffer> append_only_storage(block_storage);
        SequentialReadCharStorageOverRandomReadCharStorage sequential_read_storage(append_only_storage);
        assert(sequential_read_storage.Open().IsOk());
        for (char c = 'A'; c <= 'Z'; c++)
        {
            int cnt = ('Z' - c) * 10;
            SliceContainer container;
            assert(sequential_read_storage.Read(container, cnt).IsOk());
            int read_len;
            assert(container.GetLen(read_len).IsOk());
            assert(read_len == cnt);
            ConstSlice slice = CreateSliceFromChar(c, cnt);
            assert(container.DoesMatch(slice));
        }
    }
}

static void log()
{
    START_TEST;
    MemBlockStorage block_storage;
    ConstSlice slice1("abcdefg", 7);
    ConstSlice slice2("0123456789", 10);
    AppendOnlyCharStorage<GenericBlockBuffer> append_only_char_storage(block_storage);
    {
        LogAppender log_appender(append_only_char_storage);
        assert(log_appender.Open().IsOk());
        assert(log_appender.AppendEntry(slice1).IsOk());
        assert(log_appender.AppendEntry(slice2).IsOk());

        SequentialReadCharStorageOverRandomReadCharStorage seqread_char_storage(append_only_char_storage);
        LogReader log_reader(seqread_char_storage);
        assert(log_reader.Open().IsOk());
        SliceContainer container;
        CmpResult result;
        assert(log_reader.RetrieveNextEntry(container).IsOk());
        assert(container.Cmp(slice1, result).IsOk());
        assert(result.IsEqual());
        assert(log_reader.RetrieveNextEntry(container).IsOk());
        assert(container.Cmp(slice2, result).IsOk());
        assert(result.IsEqual());
    }
    {
        SequentialReadCharStorageOverRandomReadCharStorage seqread_char_storage(append_only_char_storage);
        LogReader log_reader(seqread_char_storage);
        assert(log_reader.Open().IsOk());
        SliceContainer container;
        CmpResult result;
        assert(log_reader.RetrieveNextEntry(container).IsOk());
        assert(container.Cmp(slice1, result).IsOk());
        assert(result.IsEqual());
        assert(log_reader.RetrieveNextEntry(container).IsOk());
        assert(container.Cmp(slice2, result).IsOk());
        assert(result.IsEqual());
    }
}

int main()
{
    append_only_storage();
    log();
    return 0;
}