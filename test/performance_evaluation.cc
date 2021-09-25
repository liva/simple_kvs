#include "utils/allocator.h"
#include "./test.h"
#include "./kvs_misc.h"
#include "misc.h"
#include "utils/rtc.h"
#include <assert.h>
#include <utility>

using namespace HayaguiKvs;

static inline void memallocator_performance()
{
    char *buf;
    {
        TimeTaker time_taker("alloc_8");
        buf = MemAllocator::alloc(8);
    }
    {
        TimeTaker time_taker("free_8");
        MemAllocator::free(buf);
    }
    {
        TimeTaker time_taker("alloc_500");
        buf = MemAllocator::alloc(500);
    }
    {
        TimeTaker time_taker("free_500");
        MemAllocator::free(buf);
    }
}

class SingleShotPerformanceMeasurer
{
public:
    SingleShotPerformanceMeasurer(KvsContainerInterface &kvs_container) : kvs_container_(kvs_container)
    {
    }
    void Do()
    {
        START_TEST;
        ConstSlice key("0123456789012345", 16);
        char buf[500];
        for (int i = 0; i < 500; i++)
        {
            buf[i] = (i % 26) + 'A';
        }
        ConstSlice value(buf, 500);
        Optional<Status> s1 = Optional<Status>::CreateInvalidObj();
        {
            TimeTaker time_taker("Put");
            Status s = kvs_container_->Put(WriteOptions(), key, value);
            s1 = Optional<Status>::CreateValidObj(std::move(s));
        }
        if (!s1.isPresent() || s1.get().IsError())
        {
            abort();
        }
        {
            TimeTaker time_taker("Get");
            SliceContainer container;
            Status s = kvs_container_->Get(ReadOptions(), key, container);
            s1 = Optional<Status>::CreateValidObj(std::move(s));
        }
        if (!s1.isPresent() || s1.get().IsError())
        {
            abort();
        }
    }

private:
    KvsContainerInterface &kvs_container_;
};

class TimeComplexityMeasurer
{
public:
    TimeComplexityMeasurer(KvsContainerInterface &kvs_container) : kvs_container_(kvs_container)
    {
    }
    void Do()
    {
        START_TEST;
        printf(",Put,Get\n");
        for (int i = 0; i < 8; i++)
        {
            printf("%d", i);
            MeasurePut(i);
            MeasureGet(i);
            printf("\n");
        }
    }

private:
    class OneRequestTimeTaker
    {
    public:
        OneRequestTimeTaker(const char *const string) : time_taker_(string)
        {
        }
        ~OneRequestTimeTaker()
        {
            time_taker_.PrintMeasuredTime();
        }

    private:
        class TimeTakerInternal final : public RtcTaker
        {
        public:
            TimeTakerInternal(const char *const string) : string_(string)
            {
            }
            virtual void Print(uint64_t time) override
            {
                printf(",%lu", time / kRequestNumPerMeasure);
            }
            const char *const string_;
        };
        TimeTakerInternal time_taker_;
    };
    void MeasurePut(const int count)
    {
        OneRequestTimeTaker time_taker("Put");
        for (int i = count * kRequestNumPerMeasure; i < (count + 1) * kRequestNumPerMeasure; i++)
        {
            char buf1[41];
            sprintf(buf1, "%040d", i);
            char buf2[41];
            sprintf(buf2, "%040d", i);
            BufferPtrSlice key(buf1, 40);
            BufferPtrSlice value(buf2, 40);
            if (kvs_container_->Put(WriteOptions(), key, value).IsError())
            {
                abort();
            }
        }
    }
    void MeasureGet(const int count)
    {
        OneRequestTimeTaker time_taker("Get");
        for (int i = count * kRequestNumPerMeasure; i < (count + 1) * kRequestNumPerMeasure; i++)
        {
            char buf[41];
            sprintf(buf, "%040d", i);
            BufferPtrSlice key(buf, 40);
            SliceContainer container;
            if (kvs_container_->Get(ReadOptions(), key, container).IsError())
            {
                abort();
            }
        }
    }
    static const int kRequestNumPerMeasure = 500;
    KvsContainerInterface &kvs_container_;
};

template <class KvsContainer>
static void test()
{
    START_TEST_WITH_POSTFIX(typeid(KvsContainer).name());
    {
        KvsContainer kvs_container;
        SingleShotPerformanceMeasurer tester(kvs_container);
        tester.Do();
        tester.Do();
    }
    {
        KvsContainer kvs_container;
        TimeComplexityMeasurer tester(kvs_container);
        tester.Do();
    }
}

int main()
{
    memallocator_performance();
    test<GenericKvsContainer<SimpleKvs>>();
    test<GenericKvsContainer<LinkedListKvs>>();
    test<HashKvsContainer>();
    test<GenericKvsContainer<SkipListKvs<12>>>();
    test<CharStorageKvsContainer>();
    return 0;
}