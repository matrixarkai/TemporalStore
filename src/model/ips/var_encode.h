// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include "string"
namespace bcache2 {
namespace ips {

// require: dts must have enough space to keep the encode res
static inline size_t EncodeVarint32(char* dst, uint32_t v) {
    unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
    static const int B = 128;
    if (v < (1 << 7)) {
        *(ptr++) = v;
    } else if (v < (1 << 14)) {
        *(ptr++) = v | B;
        *(ptr++) = v >> 7;
    } else if (v < (1 << 21)) {
        *(ptr++) = v | B;
        *(ptr++) = (v >> 7) | B;
        *(ptr++) = v >> 14;
    } else if (v < (1 << 28)) {
        *(ptr++) = v | B;
        *(ptr++) = (v >> 7) | B;
        *(ptr++) = (v >> 14) | B;
        *(ptr++) = v >> 21;
    } else {
        *(ptr++) = v | B;
        *(ptr++) = (v >> 7) | B;
        *(ptr++) = (v >> 14) | B;
        *(ptr++) = (v >> 21) | B;
        *(ptr++) = v >> 28;
    }
    return static_cast<size_t>(reinterpret_cast<char*>(ptr) - dst);
}

static inline const char* DecodeVarint32ImplFallback(const char* p, const char* limit,
                                                     uint32_t* value) {
    uint32_t result = 0;
    for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
        uint32_t byte = *(reinterpret_cast<const unsigned char*>(p));
        p++;
        if (byte & 128) {
            // More bytes are present
            result |= ((byte & 127) << shift);
        } else {
            result |= (byte << shift);
            *value = result;
            return reinterpret_cast<const char*>(p);
        }
    }
    return nullptr;
}

static inline const char* DecodeVarint32Impl(const char* p, const char* limit, uint32_t* value) {
    if (p < limit) {
        uint32_t result = *(reinterpret_cast<const unsigned char*>(p));
        if ((result & 128) == 0) {
            *value = result;
            return p + 1;
        }
    }
    return DecodeVarint32ImplFallback(p, limit, value);
}

// return cur value encode size
static inline size_t DecodeVarint32(const char* input, size_t size, uint32_t* value) {
    const char* limit = input + size;
    const char* next_input = DecodeVarint32Impl(input, limit, value);
    if (next_input == nullptr) {
        return 0;
    } else {
        return static_cast<size_t>(next_input - input);
    }
}

// require: dts must have enough space to keep the encode res
// return the encode size
static inline size_t EncodeVarint64(char* dst, uint64_t v) {
    static const unsigned int B = 128;
    unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
    while (v >= B) {
        *(ptr++) = (v & (B - 1)) | B;
        v >>= 7;
    }
    *(ptr++) = static_cast<unsigned char>(v);

    return static_cast<size_t>(reinterpret_cast<char*>(ptr) - dst);
}

const inline char* DecodeVarint64PtrImpl(const char* p, const char* limit, uint64_t* value) {
    uint64_t result = 0;
    for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
        uint64_t byte = *(reinterpret_cast<const unsigned char*>(p));
        p++;
        if (byte & 128) {
            // More bytes are present
            result |= ((byte & 127) << shift);
        } else {
            result |= (byte << shift);
            *value = result;
            return reinterpret_cast<const char*>(p);
        }
    }
    return nullptr;
}

static inline size_t DecodeVarint64(const char* input, size_t size, uint64_t* value) {
    const char* limit = input + size;
    const char* next_input = DecodeVarint64PtrImpl(input, limit, value);
    if (next_input == nullptr) {
        return 0;
    } else {
        return static_cast<size_t>(next_input - input);
    }
}

// encode signed int64
static constexpr inline uint64_t i64ToZigzag(const int64_t l) {
    return (static_cast<uint64_t>(l) << 1) ^ static_cast<uint64_t>(l >> 63);
}
static constexpr inline int64_t zigzagToI64(uint64_t n) {
    return (n >> 1) ^ -static_cast<int64_t>(n & 1);
}

static inline size_t EncodeVarsignedint64(char* dst, int64_t v) {
    // Using Zigzag format to convert signed to unsigned
    return EncodeVarint64(dst, i64ToZigzag(v));
}

static inline size_t DecodeVarsignedint64(const char* input, size_t size, int64_t* value) {
    uint64_t u = 0;
    size_t decode_size = DecodeVarint64(input, size, &u);
    *value = zigzagToI64(u);
    return decode_size;
}

// signed int32 encode && decode
static constexpr inline int32_t zigzagToI32(uint32_t n) { return (n >> 1) ^ -(n & 1); }

static constexpr inline uint32_t i32ToZigzag(const int32_t n) {
    return (static_cast<uint32_t>(n) << 1) ^ static_cast<uint32_t>(n >> 31);
}

static inline size_t EncodeVarsignedint32(char* dst, int32_t v) {
    // Using Zigzag format to convert signed to unsigned
    return EncodeVarint32(dst, i32ToZigzag(v));
}

static inline size_t DecodeVarsignedint32(const char* input, size_t size, int32_t* value) {
    uint32_t u = 0;
    size_t decode_size = DecodeVarint32(input, size, &u);
    *value = zigzagToI32(u);
    return decode_size;
}

}  // namespace ips
}  // namespace bcache2
