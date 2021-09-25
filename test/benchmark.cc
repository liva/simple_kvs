#include "utils/allocator.h"
#include "./test.h"
#include "./kvs_misc.h"
#include "misc.h"
#include "utils/rtc.h"
#include <assert.h>
#include <utility>

using namespace HayaguiKvs;

template <int kCnt>
class Measurer
{
public:
    Measurer(KvsContainerInterface &kvs_container) : kvs_container_(kvs_container)
    {
    }
    void Do()
    {
        printf(">>>num : %d\n", kCnt * kRequestNumPerMeasure);
        printf(">>>workload : seq_write\n");
        MeasurePut();
        printf(">>>num : %d\n", kCnt * kRequestNumPerMeasure);
        printf(">>>workload : seq_read\n");
        MeasureGet();
    }

private:
    void MeasurePut()
    {
        TimeTaker time_taker("time");
        for (int i = 0; i < kCnt * kRequestNumPerMeasure; i++)
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
    void MeasureGet()
    {
        TimeTaker time_taker("time");
        for (int i = 0; i < kCnt * kRequestNumPerMeasure; i++)
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


#ifndef CNT
#define CNT 0
#endif

int main()
{
    CharStorageKvsContainer kvs_container;
    Measurer<CNT> tester(kvs_container);
    tester.Do();
    return 0;
}