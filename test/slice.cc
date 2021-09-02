#include "../utils/slice.h"
#include "./test.h"
#include <stdlib.h>
#include <vector>
std::vector<int> dummy;

using namespace HayaguiKvs;

template <class S1, class S2>
void helper_compare_invalid_sliced(const S1 &slice1, const S2 &slice2)
{
    CmpResult result;
    assert(slice1.Cmp(slice2, result).IsError());
    assert(static_cast<const Slice &>(slice1).Cmp(slice2, result).IsError());
    assert(slice1.Cmp(static_cast<const Slice &>(slice2), result).IsError());
    assert(static_cast<const Slice &>(slice1).Cmp(static_cast<const Slice &>(slice2), result).IsError());
}

template <class S1, class S2>
void helper_compare_valid_sliced(const S1 &slice1, const S2 &slice2, CmpResult expected_result)
{
    CmpResult result;
    assert(slice1.Cmp(slice2, result).IsOk());
    assert(result.IsSameResult(expected_result));
    assert(static_cast<const Slice &>(slice1).Cmp(slice2, result).IsOk());
    assert(result.IsSameResult(expected_result));
    assert(slice1.Cmp(static_cast<const Slice &>(slice2), result).IsOk());
    assert(result.IsSameResult(expected_result));
    assert(static_cast<const Slice &>(slice1).Cmp(static_cast<const Slice &>(slice2), result).IsOk());
    assert(result.IsSameResult(expected_result));
}

static void cmp_invalid_slices()
{
    START_TEST;
    InvalidSlice slice1, slice2;
    helper_compare_invalid_sliced(slice1, slice2);
}

static void cmp_valid_and_invalid_slice()
{
    START_TEST;
    InvalidSlice islice;
    ConstSlice vslice("aaa", 3);
    helper_compare_invalid_sliced(islice, vslice);
    helper_compare_invalid_sliced(vslice, islice);
}

static void cmp_empty_slices()
{
    START_TEST;
    ConstSlice slice1("", 0), slice2("", 0);
    helper_compare_valid_sliced(slice1, slice2, CmpResult::CreateSameResult());
    helper_compare_valid_sliced(slice2, slice1, CmpResult::CreateSameResult());
}

static void cmp_empty_and_valid_slice()
{
    START_TEST;
    ConstSlice slice1("", 0), slice2("aaa", 3);
    helper_compare_valid_sliced(slice1, slice2, CmpResult::CreateLowerResult());
    helper_compare_valid_sliced(slice2, slice1, CmpResult::CreateGreaterResult());
}

static void cmp_same_valid_slice()
{
    START_TEST;
    ConstSlice slice1("aaa", 3), slice2("aaa", 3);
    helper_compare_valid_sliced(slice1, slice2, CmpResult::CreateSameResult());
    helper_compare_valid_sliced(slice2, slice1, CmpResult::CreateSameResult());
}

static void cmp_same_len_different_slice()
{
    START_TEST;
    ConstSlice slice1("aaa", 3), slice2("abc", 3);
    helper_compare_valid_sliced(slice1, slice2, CmpResult::CreateLowerResult());
    helper_compare_valid_sliced(slice2, slice1, CmpResult::CreateGreaterResult());
}

static void cmp_different_len_different_slice()
{
    START_TEST;
    ConstSlice slice1("aaaa", 4), slice2("abc", 3);
    helper_compare_valid_sliced(slice1, slice2, CmpResult::CreateLowerResult());
    helper_compare_valid_sliced(slice2, slice1, CmpResult::CreateGreaterResult());
}

static void cmp_substrings()
{
    START_TEST;
    ConstSlice slice1("abcd", 4), slice2("abc", 3);
    helper_compare_valid_sliced(slice1, slice2, CmpResult::CreateGreaterResult());
    helper_compare_valid_sliced(slice2, slice1, CmpResult::CreateLowerResult());
}

static void shrinked_slice() {
    START_TEST;
    ConstSlice slice1("abcde", 5), slice2("xybcd", 5);
    ShrinkedSlice shrinked_slice1(slice1, 1, 3);
    ShrinkedSlice shrinked_slice2(slice2, 2, 3);
    ShrinkedSlice shrinked_slice3(slice1, 1, 4);
    helper_compare_valid_sliced(shrinked_slice1, shrinked_slice2, CmpResult::CreateSameResult());
    helper_compare_valid_sliced(shrinked_slice3, shrinked_slice2, CmpResult::CreateGreaterResult());
}

static void container_set()
{
    START_TEST;
    SliceContainer sc;
    ConstSlice slice("aaa", 3);
    sc.Set(slice);
    assert(sc.DoesMatch(slice));
}

static void container_get_validslice()
{
    START_TEST;
    SliceContainer sc;
    ConstSlice slice("abcd", 4);
    sc.Set(slice);
    assert(sc.IsSliceAvailable());
    assert(sc.CreateConstSlice().DoesMatch(slice));
}

int main()
{
    cmp_invalid_slices();
    cmp_valid_and_invalid_slice();
    cmp_empty_slices();
    cmp_empty_and_valid_slice();
    cmp_same_valid_slice();
    cmp_same_len_different_slice();
    cmp_different_len_different_slice();
    cmp_substrings();
    shrinked_slice();
    container_set();
    container_get_validslice();
    return 0;
}