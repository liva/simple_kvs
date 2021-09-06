#include "kvs/simple_kvs.h"
#include "kvs/block_storage_kvs.h"
#include "block_storage/file_block_storage.h"
#include "./test.h"
#include "misc.h"
#include <assert.h>
#include <utility>

class VeRtcTaker
{
public:
    VeRtcTaker() : x_(get())
    {
    }
    void PrintMeasuredTime()
    {
        Print(get() - x_);
    }
    static inline uint64_t get()
    {
        uint64_t ret;
        void *vehva = ((void *)0x000000001000);
        asm volatile("" ::
                         : "memory");
        asm volatile("lhm.l %0,0(%1)"
                     : "=r"(ret)
                     : "r"(vehva));
        asm volatile("" ::
                         : "memory");
        // the "800" is due to the base frequency of Tsubasa
        return ((uint64_t)1000 * ret) / 800;
    }

protected:
    virtual void Print(uint64_t time) = 0;

private:
    const uint64_t x_;
};

class GetTimeTaker
{
public:
    GetTimeTaker()
    {
    }
    ~GetTimeTaker()
    {
        time_taker_.PrintMeasuredTime();
    }

private:
    class TimeTakerInternal final : public VeRtcTaker
    {
    public:
        virtual void Print(uint64_t time) override
        {
            printf(">>Get %luns\n", time);
        }
    };
    TimeTakerInternal time_taker_;
};

class PutTimeTaker
{
public:
    PutTimeTaker()
    {
    }
    ~PutTimeTaker()
    {
        time_taker_.PrintMeasuredTime();
    }
private:
    class TimeTakerInternal final : public VeRtcTaker
    {
    public:
        virtual void Print(uint64_t time) override
        {
            printf(">>Put %luns\n", time);
        }
    };
    TimeTakerInternal time_taker_;
};

using namespace HayaguiKvs;

static inline void singleshot_performance()
{
    START_TEST;
    File file;
    file.Init();
    FileBlockStorage block_storage(file.fname_);
    SimpleKvs cache_kvs;
    BlockStorageKvs<GenericBlockBuffer> block_storage_kvs(block_storage, cache_kvs);
    ConstSlice key("0123456789012345", 16);
    char buf[500];
    for (int i = 0; i < 500; i++)
    {
        buf[i] = (i % 26) + 'A';
    }
    ConstSlice value(buf, 500);
    Optional<Status> s1 = Optional<Status>::CreateInvalidObj();
    {
        PutTimeTaker time_taker;
        Status s = block_storage_kvs.Put(WriteOptions(), key, value);
        s1 = Optional<Status>::CreateValidObj(std::move(s));
    }
    if (!s1.isPresent() || s1.get().IsError())
    {
        abort();
    }
    {
        GetTimeTaker time_taker;
        SliceContainer container;
        Status s = block_storage_kvs.Get(ReadOptions(), key, container);
        s1 = Optional<Status>::CreateValidObj(std::move(s));
    }
    if (!s1.isPresent() || s1.get().IsError())
    {
        abort();
    }
}

int main()
{
    singleshot_performance();
    return 0;
}