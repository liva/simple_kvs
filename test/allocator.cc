#include "../utils/allocator.h"
#include "test.h"
#include <assert.h>
#include <vector>
std::vector<int> dummy;

using namespace HayaguiKvs;

class TestMemAllocator : public MallocBasedMemAllocator
{
public:
    virtual ~TestMemAllocator()
    {
        assert(allocated_ == 0);
    }
    virtual void *alloc(size_t len) override
    {
        allocated_ += len;
        void *buf = MallocBasedMemAllocator::alloc(len + 8);
        *((uint64_t *)buf) = len;
        return (void *)((char *)buf + 8);
    }
    virtual void free(void *buf) override
    {
        buf = (void *)((char *)buf - 8);
        allocated_ -= *((uint64_t *)buf);
        MallocBasedMemAllocator::free(buf);
    }

private:
    size_t allocated_ = 0;
};

void InitBuffer(void *buf, uint8_t i, size_t len)
{
    for (int j = 0; j < len; j++)
    {
        ((uint8_t *)buf)[j] = i;
    }
}

void CheckBuffer(void *buf, uint8_t i, size_t len)
{
    for (int j = 0; j < len; j++)
    {
        assert(((uint8_t *)buf)[j] == i);
    }
}

static void single_alloc_free()
{
    START_TEST;
    TestMemAllocator base_allocator;
    LocalBufferAllocator *allocator = LocalBufferAllocator::ResetWithBaseAllocator(base_allocator);

    {
        LocalBufferAllocator::Container container1 = allocator->Alloc(32);
        void *buf1 = container1.GetPtr();
        InitBuffer(buf1, 1, 32);
        LocalBufferAllocator::Container container2 = allocator->Alloc(64);
        void *buf2 = container2.GetPtr();
        InitBuffer(buf2, 2, 64);
        CheckBuffer(buf1, 1, 32);
        CheckBuffer(buf2, 2, 64);
    }

    LocalBufferAllocator::ResetWithDefaultAllocator();
}

static void allocate_large_buffer()
{
    START_TEST;
    TestMemAllocator base_allocator;
    LocalBufferAllocator *allocator = LocalBufferAllocator::ResetWithBaseAllocator(base_allocator);

    {
        LocalBufferAllocator::Container container1 = allocator->Alloc(4096);
        void *buf1 = container1.GetPtr();
        InitBuffer(buf1, 1, 4096);
        LocalBufferAllocator::Container container2 = allocator->Alloc(4096);
        void *buf2 = container2.GetPtr();
        InitBuffer(buf2, 2, 4096);
        CheckBuffer(buf1, 1, 4096);
        CheckBuffer(buf2, 2, 4096);
    }

    LocalBufferAllocator::ResetWithDefaultAllocator();
}

static void allocate_many_buffers()
{
    START_TEST;
    TestMemAllocator base_allocator;
    LocalBufferAllocator *allocator = LocalBufferAllocator::ResetWithBaseAllocator(base_allocator);

    for(int i = 0; i < 1024; i++){
        LocalBufferAllocator::Container container1 = allocator->Alloc(64);
        void *buf1 = container1.GetPtr();
        InitBuffer(buf1, i % 0xFF, 4096);
    }

    LocalBufferAllocator::ResetWithDefaultAllocator();
}

int main()
{
    single_alloc_free();
    allocate_large_buffer();
    allocate_many_buffers();
    return 0;
}