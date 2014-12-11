
#include <string.h>
#include <stdint.h>

#include "gost_hash.h"
namespace {
uint_fast32_t sub(const uint_fast32_t sbox[4][256], uint_fast32_t x)
{
    return sbox[0][x /***/ & 255] ^
           sbox[1][x >>  8 & 255] ^
           sbox[2][x >> 16 & 255] ^
           sbox[3][x >> 24 & 255];
}
uint64_t encrypt(const uint_fast32_t sbox[4][256], const uint_fast32_t key[8], uint64_t block)
{
    uint_fast32_t a = (uint_fast32_t)block;
    uint_fast32_t b = block >> 32;
    b ^= sub(sbox, a + key[0]); a ^= sub(sbox, b + key[1]);
    b ^= sub(sbox, a + key[2]); a ^= sub(sbox, b + key[3]);
    b ^= sub(sbox, a + key[4]); a ^= sub(sbox, b + key[5]);
    b ^= sub(sbox, a + key[6]); a ^= sub(sbox, b + key[7]);
    b ^= sub(sbox, a + key[0]); a ^= sub(sbox, b + key[1]);
    b ^= sub(sbox, a + key[2]); a ^= sub(sbox, b + key[3]);
    b ^= sub(sbox, a + key[4]); a ^= sub(sbox, b + key[5]);
    b ^= sub(sbox, a + key[6]); a ^= sub(sbox, b + key[7]);
    b ^= sub(sbox, a + key[0]); a ^= sub(sbox, b + key[1]);
    b ^= sub(sbox, a + key[2]); a ^= sub(sbox, b + key[3]);
    b ^= sub(sbox, a + key[4]); a ^= sub(sbox, b + key[5]);
    b ^= sub(sbox, a + key[6]); a ^= sub(sbox, b + key[7]);
    b ^= sub(sbox, a + key[7]); a ^= sub(sbox, b + key[6]);
    b ^= sub(sbox, a + key[5]); a ^= sub(sbox, b + key[4]);
    b ^= sub(sbox, a + key[3]); a ^= sub(sbox, b + key[2]);
    b ^= sub(sbox, a + key[1]); a ^= sub(sbox, b + key[0]);
    return uint64_t(a) << 32 | b;
}
void mixing_transform(uint16_t data[16])
{
    uint_fast16_t temp = data[0] ^ data[1] ^ data[2] ^ data[3] ^ data[12] ^ data[15];
    memmove(data, data + 1, 30);
    data[15] = temp;
}
void add_bytes(const uint64_t src[4], uint64_t dst[4])
{
    dst[0] += src[0]; dst[1] += src[1];
    dst[2] += src[2]; dst[3] += src[3];
    if (dst[2] < src[2]) ++dst[3];
    if (dst[1] < src[1]) if (!++dst[2]) ++dst[3];
    if (dst[0] < src[0]) if (!++dst[1]) if (!++dst[2]) ++dst[3];
}
void hash_step(const uint_fast32_t sbox[4][256], const uint64_t m[4], uint64_t h[4])
{
    uint64_t u[4], v[4], temp;
    uint_fast32_t k[8];
    union {
        uint64_t _64[ 4];
        uint8_t  _8 [32];
    } w;
    union {
        uint64_t _64[ 4];
        uint16_t _16[16];
    } s;
    memcpy(u, h, 32);
    memcpy(v, m, 32);
    for (int i = 0; i != 4; ++i) {
        w._64[0] = u[0] ^ v[0];
        w._64[1] = u[1] ^ v[1];
        w._64[2] = u[2] ^ v[2];
        w._64[3] = u[3] ^ v[3];
        k[0] = w._8[24] << 24 | w._8[16] << 16 | w._8[ 8] << 8 | w._8[0];
        k[1] = w._8[25] << 24 | w._8[17] << 16 | w._8[ 9] << 8 | w._8[1];
        k[2] = w._8[26] << 24 | w._8[18] << 16 | w._8[10] << 8 | w._8[2];
        k[3] = w._8[27] << 24 | w._8[19] << 16 | w._8[11] << 8 | w._8[3];
        k[4] = w._8[28] << 24 | w._8[20] << 16 | w._8[12] << 8 | w._8[4];
        k[5] = w._8[29] << 24 | w._8[21] << 16 | w._8[13] << 8 | w._8[5];
        k[6] = w._8[30] << 24 | w._8[22] << 16 | w._8[14] << 8 | w._8[6];
        k[7] = w._8[31] << 24 | w._8[23] << 16 | w._8[15] << 8 | w._8[7];
        s._64[i] = encrypt(sbox, k, h[i]);
        if (i == 3) break;
        temp = u[0] ^ u[1];
        u[0] = u[1];
        u[1] = u[2];
        u[2] = u[3];
        u[3] = temp;
        if (i == 1) {
            u[0] ^= 0xff00ff00ff00ff00ULL;
            u[1] ^= 0x00ff00ff00ff00ffULL;
            u[2] ^= 0xff0000ff00ffff00ULL;
            u[3] ^= 0xff00ffff000000ffULL;
        }
        temp = v[0] ^ v[1];
        v[0] = v[2];
        v[2] = temp;
        temp = v[1];
        v[1] = v[3];
        v[3] = v[0] ^ temp;
    }
    for (int i = 0; i != 12; ++i) mixing_transform(s._16);
    s._64[0] ^= m[0];
    s._64[1] ^= m[1];
    s._64[2] ^= m[2];
    s._64[3] ^= m[3];
        
    mixing_transform(s._16);
    s._64[0] ^= h[0];
    s._64[1] ^= h[1];
    s._64[2] ^= h[2];
    s._64[3] ^= h[3];
    for (int i = 0; i != 61; ++i) mixing_transform(s._16);
    memcpy(h, &s, 32);
}
uint_fast32_t rol(uint32_t x, unsigned n)
{
    return x << n | x >> (32 - n);
}
} // namespace
void Hash::Gost_ctx::init(const char start_hash[32], const uint32_t sbox[16][8])
{
	const char my_start_hash_bytes[32] = {0};
	const char my_start_s_box_values[] = {\
		4,10,9,2,13,8,0,14,6,11,1,12,7,15,5,3,\
		14,11,4,12,6,13,15,10,2,3,8,1,0,7,5,9,\
		5,8,1,13,10,3,4,2,14,15,12,7,6,0,9,11,\
		7,13,10,1,0,8,9,15,14,4,6,12,11,2,5,3,\
		6,12,7,1,5,15,13,8,4,10,9,14,0,3,11,2,\
		4,11,10,0,7,2,1,13,3,6,8,5,9,12,15,14,\
		13,11,4,1,3,15,5,9,0,10,14,7,6,8,2,12,\
		1,15,13,0,5,7,10,4,9,2,3,14,6,11,8,12};
	int k = 0;
	uint32_t my_sbox[16][8];
	for (int j = 0; j != 8; ++j)
	for (int i = 0; i != 16; ++i)
	{
		my_sbox[i][j] = my_start_s_box_values[k];
		k++;
	}
	if (start_hash == NULL)
		start_hash = my_start_hash_bytes;
	if (sbox == NULL)
		sbox = my_sbox;
    memcpy(hash, start_hash, 32);
    memset(sum, 0, 32);
    memset(len, 0, 32);
    for (int i = 0; i != 256; ++i) {
        this->sbox[0][i] = rol(sbox[i % 16][0] | sbox[i / 16][1] << 4, 11);
        this->sbox[1][i] = rol(sbox[i % 16][2] | sbox[i / 16][3] << 4, 19);
        this->sbox[2][i] = rol(sbox[i % 16][4] | sbox[i / 16][5] << 4, 27);
        this->sbox[3][i] = rol(sbox[i % 16][6] | sbox[i / 16][7] << 4,  3);
    }
}
void Hash::Gost_ctx::update(const unsigned char *input, uint64_t size)
{
    len[0] += size;
    if (len[0] < size) if (!++len[1]) if (!++len[2]) ++len[3];
    uint64_t block[4];
    uint64_t i = 0;
    if (size >= 32) {
        size -= 32;
        for (; i <= size; i += 32) {
            memcpy(block, input + i, 32);
            hash_step(sbox, block, hash);
            add_bytes(block, sum);
        }
    }
    if (size %= 32) {
        memset(block, 0, 32);
        memcpy(block, input + i, (size_t)size);
        hash_step(sbox, block, hash);
        add_bytes(block, sum);
    }
}
void Hash::Gost_ctx::finish(unsigned char final_hash[32])
{
    len[3] <<= 3; len[3] |= len[2] >> 61;
    len[2] <<= 3; len[2] |= len[1] >> 61;
    len[1] <<= 3; len[1] |= len[0] >> 61;
    len[0] <<= 3; /*********************/
    hash_step(sbox, len, hash);
    hash_step(sbox, sum, hash);
    memcpy(final_hash, hash, 32);
}
