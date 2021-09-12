#include "utils/allocator.h"
#include "./test.h"
#include "./kvs_misc.h"
#include "misc.h"
#include "utils/ve_rtc.h"
#include <assert.h>
#include <utility>

using namespace HayaguiKvs;

class TimeTaker
{
public:
    TimeTaker(const char *const string) : time_taker_(string)
    {
    }
    ~TimeTaker()
    {
        time_taker_.PrintMeasuredTime();
    }

private:
    class TimeTakerInternal final : public VeRtcTaker
    {
    public:
        TimeTakerInternal(const char *const string) : string_(string)
        {
        }
        virtual void Print(uint64_t time) override
        {
            printf(">>%s %luns\n", string_, time);
        }
        const char *const string_;
    };
    TimeTakerInternal time_taker_;
};

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

class SingleShotPerformanceTester
{
public:
    SingleShotPerformanceTester(KvsContainerInterface &kvs_container) : kvs_container_(kvs_container)
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

int main()
{
    memallocator_performance();
    {
        GenericKvsContainer<SimpleKvs> kvs_container;
        SingleShotPerformanceTester tester(kvs_container);
        tester.Do();
    }
    {
        GenericKvsContainer<LinkedListKvs> kvs_container;
        SingleShotPerformanceTester tester(kvs_container);
        tester.Do();
    }
    {
        HashKvsContainer kvs_container;
        SingleShotPerformanceTester tester(kvs_container);
        tester.Do();
    }
    {
        BlockStoragKvsContainer1 kvs_container;
        SingleShotPerformanceTester tester(kvs_container);
        tester.Do();
    }
    {
        BlockStoragKvsContainer2 kvs_container;
        SingleShotPerformanceTester tester(kvs_container);
        tester.Do();
    }
    {
        BlockStoragKvsContainer kvs_container;
        SingleShotPerformanceTester tester(kvs_container);
        tester.Do();
    }
    return 0;
}