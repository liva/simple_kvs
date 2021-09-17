/*
Copyright (c) 2011 The LevelDB Authors. All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

   * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
   * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
   * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

#pragma once

#include <stdint.h>
#include <assert.h>

namespace HayaguiKvs
{

    // A very simple random number generator.  Not especially good at
    // generating truly random bits, but good enough for our needs in this
    // package.
    class Random
    {
    private:
        uint32_t seed_;

    public:
        explicit Random(uint32_t s) : seed_(s & 0x7fffffffu)
        {
            // Avoid bad seeds.
            if (seed_ == 0 || seed_ == 2147483647L)
            {
                seed_ = 1;
            }
        }
        uint32_t Next()
        {
            static const uint32_t M = 2147483647L; // 2^31-1
            static const uint64_t A = 16807;       // bits 14, 8, 7, 5, 2, 1, 0
            // We are computing
            //       seed_ = (seed_ * A) % M,    where M = 2^31-1
            //
            // seed_ must not be zero or M, or else all subsequent computed values
            // will be zero or M respectively.  For all other values, seed_ will end
            // up cycling through every number in [1,M-1]
            uint64_t product = seed_ * A;

            // Compute (product % M) using the fact that ((x << 31) % M) == x.
            seed_ = static_cast<uint32_t>((product >> 31) + (product & M));
            // The first reduction may overflow by 1 bit, so we may need to
            // repeat.  mod == M is not possible; using > allows the faster
            // sign-bit-based test.
            if (seed_ > M)
            {
                seed_ -= M;
            }
            return seed_;
        }
        // Returns a uniformly distributed value in the range [0..n-1]
        // REQUIRES: n > 0
        uint32_t Uniform(int n) { return Next() % n; }

        // Randomly returns true ~"1/n" of the time, and false otherwise.
        // REQUIRES: n > 0
        bool OneIn(int n) { return (Next() % n) == 0; }

        // Skewed: pick "base" uniformly from range [0,max_log] and then
        // return "base" random bits.  The effect is to pick a number in the
        // range [0,2^max_log-1] with exponential bias towards smaller numbers.
        uint32_t Skewed(int max_log) { return Uniform(1 << Uniform(max_log + 1)); }
    };

    int RandomHeight(const int max_height, Random &rnd)
    {
        // Increase height with probability 1 in kBranching
        static const unsigned int kBranching = 4;
        int height = 1;
        while (height < max_height && ((rnd.Next() % kBranching) == 0))
        {
            height++;
        }
        assert(height > 0);
        assert(height <= max_height);
        return height;
    }

}