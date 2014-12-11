#include <stdint.h>
#pragma once

namespace Hash {

struct Gost_ctx {
    void init(const char start_hash[32], const uint32_t sbox[16][8]);
    void update(const unsigned char *input = 0, uint64_t size = 0);
    void finish(unsigned char final_hash[32]);

private:
    uint_fast32_t sbox[4][256];

    uint64_t hash[4], sum[4], len[4];
};

} // namespace Hash
