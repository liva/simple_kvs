#include "block_storage/memblock_storage.h"
#include "char_storage/char_storage_over_blockstorage.h"
#include "char_storage/vefs.h"
#include "char_storage/log.h"
#include "./test.h"
#include "test_storage.h"
#include "misc.h"
#include <memory>
#include <vector>
std::vector<int> dummy;

using namespace HayaguiKvs;

static void append_only_storage(AppendOnlyCharStorageInterface &char_storage)
{
    START_TEST;
    {
        assert(char_storage.Open().IsOk());
        for (char c = 'A'; c <= 'Z'; c++)
        {
            int cnt = ('Z' - c) * 10;
            ConstSlice slice = CreateSliceFromChar(c, cnt);
            size_t prev_len = char_storage.GetLen();
            assert(char_storage.Append(slice).IsOk());
            assert(char_storage.GetLen() == prev_len + cnt);
        }
        SequentialReadCharStorageOverRandomReadCharStorage sequential_read_storage(char_storage);
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
        SequentialReadCharStorageOverRandomReadCharStorage sequential_read_storage(char_storage);
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

static void check_cache_of_append_only_storage()
{
    START_TEST;
    TestStorage block_storage;
    AppendOnlyCharStorageOverBlockStorage<GenericBlockBuffer> append_only_storage(block_storage);
    assert(append_only_storage.Open().IsOk());
    block_storage.ResetCnt();

    ConstSlice slice1 = CreateSliceFromChar('a', BlockBufferInterface::kSize);
    assert(append_only_storage.Append(slice1).IsOk());
    assert(block_storage.IsWriteCntAdded(2)); // 1 for data write, 1 for meta data update
    assert(block_storage.IsReadCntAdded(0));

    ConstSlice slice2 = CreateSliceFromChar('b', BlockBufferInterface::kSize / 2);
    assert(append_only_storage.Append(slice2).IsOk());
    assert(block_storage.IsWriteCntAdded(2)); // same as well
    assert(block_storage.IsReadCntAdded(0));

    ConstSlice slice3 = CreateSliceFromChar('c', BlockBufferInterface::kSize);
    assert(append_only_storage.Append(slice3).IsOk());
    assert(block_storage.IsWriteCntAdded(3)); // 2 for data write(the data lies upon two blocks), 1 for meta data update
    assert(block_storage.IsReadCntAdded(0));
}

static void log()
{
    START_TEST;
    MemBlockStorage block_storage;
    ConstSlice slice1("abcdefg", 7);
    ConstSlice slice2("0123456789", 10);
    AppendOnlyCharStorageOverBlockStorage<GenericBlockBuffer> append_only_char_storage(block_storage);
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
    {
        MemBlockStorage block_storage;
        AppendOnlyCharStorageOverBlockStorage<GenericBlockBuffer> char_storage(block_storage);
        append_only_storage(char_storage);
    }
    {
        VefsFile file;
        VefsCharStorage char_storage(file.fname_);
        append_only_storage(char_storage);
    }
    check_cache_of_append_only_storage();
    log();
    return 0;
}