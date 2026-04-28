#include <iostream>
#include <fstream>
#include <cstdlib>
#include <cstdint>
#include <string>
#include <cstring>
#include <vector>
#include  "rdb.h"
#include  "ziplist.h"
#include "endianconv.h"
#include "assert.h"
#include  "lzf.h"
using namespace std;

struct Buffer {
    FILE* fp;
    uint8_t buf[4096];
    int pos;
    int size;
};

void buffer_init(Buffer* b, FILE* fp) {
    b->fp = fp;
    b->pos = 0;
    b->size = 0;
}

uint8_t buffer_read_byte(Buffer* b) {
    if (b->pos >= b->size) {
        b->size = fread(b->buf, 1, sizeof(b->buf), b->fp);
        b->pos = 0;
        if (b->size == 0) {
            cout << "文件结束" << endl;
            exit(1);
        }
    }
    return b->buf[b->pos++];
}

unsigned char buffer_peek_byte(Buffer *buf) {
    if (buf->pos >= buf->size) return 0;
    return buf->buf[buf->pos];
}

bool buffer_peek_bytes(Buffer *buf,uint8_t* out,int len) {
    if (buf->pos >= buf->size-len) return false;
    for (int i = 0; i < len; i++) {
        out[i] = buf->buf[buf->pos++];
    }
    return true;
}

void buffer_read_bytes(Buffer* b, uint8_t* out, int len) {
    for (int i = 0; i < len; i++) {
        out[i] = buffer_read_byte(b);
    }
}

void buffer_read_chars(Buffer* b, unsigned char* out, int len) {
    for (int i = 0; i < len; i++) {
        out[i] = buffer_read_byte(b);
    }
}

uint32_t buffer_read_u32(Buffer* b) {
    uint32_t res = 0;
    res |= buffer_read_byte(b) << 24;
    res |= buffer_read_byte(b) << 16;
    res |= buffer_read_byte(b) << 8;
    res |= buffer_read_byte(b);
    return res;
}

uint64_t buffer_read_u64(Buffer* b) {
    uint64_t hi = buffer_read_u32(b);
    uint64_t lo = buffer_read_u32(b);
    return (hi << 32) | lo;
}

bool rdbLoadLenEx(Buffer* buf, uint64_t* len, bool* isencoded) {
    uint8_t b = buffer_read_byte(buf);
    int type = (b >> 6) & 0x03;
    *isencoded = false;

    if (type == RDB_6BITLEN) {
        *len = b & 0x3F;
        return true;
    }
    if (type == RDB_14BITLEN) {
        *len = ((uint64_t)(b & 0x3F) << 8) | buffer_read_byte(buf);
        return true;
    }
    if (type == 2) {
        if ((b & 0x3F) == 0) {
            *len = buffer_read_u32(buf);
            return true;
        }
        if ((b & 0x3F) == 1) {
            *len = buffer_read_u64(buf);
            return true;
        }
        return false;
    }

    *isencoded = true;
    *len = b & 0x3F;
    return true;
}

string read_string(Buffer* buf) {
    uint64_t len = 0;
    bool isencoded = false;
    if (!rdbLoadLenEx(buf, &len, &isencoded)) {
        return "invalid_len";
    }

    if (!isencoded) {
        string s(len, 0);
        if (len > 0) {
            buffer_read_bytes(buf, (uint8_t*)&s[0], (int)len);
        }
        return s;
    }

    long long num = 0;
    if (len == RDB_ENC_INT8) {
        num = (int8_t)buffer_read_byte(buf);
        return to_string(num);
    }
    if (len == RDB_ENC_INT16) {
        int16_t v = (int16_t)(buffer_read_byte(buf) | (buffer_read_byte(buf) << 8));
        return to_string(v);
    }
    if (len == RDB_ENC_INT32) {
        int32_t v = (int32_t)(buffer_read_byte(buf) |
            (buffer_read_byte(buf) << 8) |
            (buffer_read_byte(buf) << 16) |
            (buffer_read_byte(buf) << 24));
        return to_string(v);
    }
    if (len == RDB_ENC_LZF) {
        uint64_t compressed_len = 0;
        uint64_t origin_len = 0;
        bool encoded2 = false;
        if (!rdbLoadLenEx(buf, &compressed_len, &encoded2) || encoded2) return "invalid_lzf_len";
        if (!rdbLoadLenEx(buf, &origin_len, &encoded2) || encoded2) return "invalid_lzf_len";

        uint8_t* compressed = new uint8_t[compressed_len];
        buffer_read_bytes(buf, compressed, (int)compressed_len);
        uint8_t* origin = new uint8_t[origin_len];
        unsigned int decompressed = lzf_decompress(compressed, (unsigned int)compressed_len, origin, (unsigned int)origin_len);
        string out((char*)origin, decompressed);
        delete[] compressed;
        delete[] origin;
        return out;
    }

    return "unknown_int";
}

// 读取 Redis 长度编码（对应 rdbSaveLen）
// 返回：读到的长度
uint64_t rdbLoadLen(Buffer* buf) {
    uint64_t len = 0;
    bool isencoded = false;
    if (!rdbLoadLenEx(buf, &len, &isencoded) || isencoded) {
        return RDB_LENERR;
    }
    return len;
}

uint64_t rdbLoadMillisecondTime(Buffer* buf) {
    // 1. 读取 8 个字节 (小端)
    uint64_t le_time = 0;
    for (int i = 0; i < 8; i++) {
        le_time |= (uint64_t)buffer_read_byte(buf) << (i * 8);
    }
    // 直接就是小端解析后的时间戳（毫秒）
    return le_time;
}

// 读取 LFU 频率（热度）
uint8_t rdbLoadLFUFreq(Buffer* buf) {
    // 就 1 字节！
    return buffer_read_byte(buf);
}

void rdbLoadInfoAuxFields(Buffer* buff);

bool loadDb(Buffer* buf);

void parseZiplist(Buffer* buf);

void zipEntry(unsigned char *p, zlentry *e) {

    ZIP_DECODE_PREVLEN(p, e->prevrawlensize, e->prevrawlen);
    ZIP_DECODE_LENGTH(p + e->prevrawlensize, e->encoding, e->lensize, e->len);
    e->headersize = e->prevrawlensize + e->lensize;
    e->p = p;
}

int main(int argc, char* argv[]) {
    // ====================== 修复点1：参数必须是 2 个
    if (argc != 2) {
        cout << "用法: " << argv[0] << " dump.rdb" << endl;
        exit(1);
    }

    FILE* rdbfile = fopen(argv[1], "rb");
    if (nullptr == rdbfile) {
        cout << "读取rdb文件失败，请检查文件是否存在" << endl;
        exit(1);
    }

    // ====================== 修复点2：创建实体，不要用野指针
    Buffer buf; // 定义实体，不是指针
    buffer_init(&buf, rdbfile); // 传地址

    unsigned char header[9];
    buffer_read_chars(&buf, header, 9);
    cout << "RDB magic: " << header << endl;
    uint8_t op;
    while (true) {
        op = buffer_read_byte(&buf);
        if (op == RDB_OPCODE_AUX) {
            rdbLoadInfoAuxFields(&buf);
            continue;
        }
        else if (op == RDB_OPCODE_SELECTDB) {
            bool eof = loadDb(&buf);
            if (eof) {
                cout << "rdb文件解析完成" << endl;
                break;
            }
            continue;;
        }
        else if (op == RDB_OPCODE_EOF) {
            cout << "rdb文件解析完成" << endl;
            break;
        }
        else {
            cout << "rdb文件提前解析结束" << endl;
            break;
        }
    }
    fclose(rdbfile);
    return 0;
}

// ===================== 工具函数：小端字节序转换（Redis 用小端） =====================
uint32_t read_le32(const uint8_t* p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}
uint16_t read_le16(const uint8_t* p) {
    return (uint16_t)p[0] | ((uint16_t)p[1] << 8);
}

int64_t zipLoadInteger(unsigned char *p, unsigned char encoding) {
    int16_t i16;
    int32_t i32;
    int64_t i64, ret = 0;
    if (encoding == ZIP_INT_8B) {
        ret = ((int8_t*)p)[0];
    } else if (encoding == ZIP_INT_16B) {
        memcpy(&i16,p,sizeof(i16));
        memrev16ifbe(&i16);
        ret = i16;
    } else if (encoding == ZIP_INT_32B) {
        memcpy(&i32,p,sizeof(i32));
        memrev32ifbe(&i32);
        ret = i32;
    } else if (encoding == ZIP_INT_24B) {
        i32 = 0;
        memcpy(((uint8_t*)&i32)+1,p,sizeof(i32)-sizeof(uint8_t));
        memrev32ifbe(&i32);
        ret = i32>>8;
    } else if (encoding == ZIP_INT_64B) {
        memcpy(&i64,p,sizeof(i64));
        memrev64ifbe(&i64);
        ret = i64;
    } else if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX) {
        ret = (encoding & ZIP_INT_IMM_MASK)-1;
    } else {
        assert(NULL);
    }
    return ret;
}

void rdbLoadInfoAuxFields(Buffer* buff) {
    string aux_key = read_string(buff);
    string aux_val = read_string(buff);
    cout << "[AUX] " << aux_key << " = " << aux_val << endl;
}

void parseString(Buffer* buf);

void parseQuicklist(Buffer* buf);

void parseHashZiplist(Buffer* buf);

void parseList(Buffer* buf);

void parseSet(Buffer* buf);

void parseSetIntset(Buffer* buf);

void parseHash(Buffer* buf);

bool loadDb(Buffer* buf) {
    uint64_t dbNum = rdbLoadLen(buf);
    cout << "select db " << dbNum << endl;
    uint8_t op = buffer_read_byte(buf);
    uint64_t db_size, expires_size;
    db_size = rdbLoadLen(buf);
    expires_size = rdbLoadLen(buf);
    cout << "RDB_OPCODE " << (int)op << " db_size " << db_size << " expires_size " << expires_size << endl;
    uint8_t type;
    while (true) {
        long long expiretime = 0;
        uint64_t idletime = 0;
        uint8_t lfu_freq = 0;
        type = buffer_read_byte(buf);

        while (type == RDB_OPCODE_EXPIRETIME_MS || type == RDB_OPCODE_EXPIRETIME ||
               type == RDB_OPCODE_IDLE || type == RDB_OPCODE_FREQ) {
            if (type == RDB_OPCODE_EXPIRETIME_MS) {
                expiretime = rdbLoadMillisecondTime(buf);
            } else if (type == RDB_OPCODE_EXPIRETIME) {
                expiretime = (long long)buffer_read_u32(buf) * 1000;
            } else if (type == RDB_OPCODE_IDLE) {
                idletime = rdbLoadLen(buf) * 1000;
            } else if (type == RDB_OPCODE_FREQ) {
                lfu_freq = rdbLoadLFUFreq(buf);
            }
            type = buffer_read_byte(buf);
        }

        if (type == RDB_TYPE_STRING) {
            parseString(buf);
        } else if (type == RDB_TYPE_LIST) {
            parseList(buf);
        } else if (type == RDB_TYPE_SET) {
            parseSet(buf);
        } else if (type == RDB_TYPE_HASH) {
            parseHash(buf);
        } else if (type == RDB_TYPE_SET_INTSET) {
            parseSetIntset(buf);
        } else if (type == RDB_TYPE_HASH_ZIPLIST){
            parseHashZiplist(buf);
        } else if (type == RDB_TYPE_LIST_QUICKLIST) {
            parseQuicklist(buf);
        } else if (type == RDB_OPCODE_EOF) {
            return true;
        } else {
            cout << "暂不支持的数据类型：" << (int)type << endl;
            return false;
        }
        if (expiretime > 0) {
            cout << "expiretime：" << expiretime;
        }
        if (idletime > 0) {
            cout << "idletime：" << idletime;
        }
        if (lfu_freq > 0) {
            cout << "lfu_freq：" << lfu_freq;
        }
        cout << endl;
    }
    return false;
}

void parseString(Buffer* buf) {
    string key = read_string(buf);
    string value = read_string(buf);
    cout << "[string] " << "key:" << key << " value:" << value << " ";
}

vector<string> parseZiplistEntries(const string& blob) {
    vector<string> entries;
    if (blob.size() < ZIPLIST_HEADER_SIZE + ZIPLIST_END_SIZE) {
        return entries;
    }

    const unsigned char* base = (const unsigned char*)blob.data();
    uint32_t zlbytes = read_le32(base);
    if (zlbytes > blob.size()) {
        zlbytes = (uint32_t)blob.size();
    }
    const unsigned char* p = base + ZIPLIST_HEADER_SIZE;
    const unsigned char* end = base + zlbytes;

    while (p < end && *p != ZIP_END) {
        unsigned int prevlensize = 0, prevlen = 0;
        ZIP_DECODE_PREVLEN(p, prevlensize, prevlen);
        p += prevlensize;
        if (p >= end) break;

        unsigned int encoding = 0, lensize = 0, entry_len = 0;
        ZIP_DECODE_LENGTH(p, encoding, lensize, entry_len);
        if (ZIP_IS_STR(encoding)) {
            p += lensize;
            if (p + entry_len > end) break;
            entries.emplace_back((const char*)p, entry_len);
            p += entry_len;
        } else {
            p += lensize;
            if (p + entry_len > end) break;
            int64_t val = zipLoadInteger((unsigned char*)p, (unsigned char)encoding);
            entries.emplace_back(to_string(val));
            p += entry_len;
        }
    }

    return entries;
}

void parseQuicklist(Buffer* buf) {
    string key = read_string(buf);
    uint64_t nodeSize = rdbLoadLen(buf);
    vector<string> allEntries;
    while (nodeSize > 0) {
        string ziplistBlob = read_string(buf);
        vector<string> entries = parseZiplistEntries(ziplistBlob);
        allEntries.insert(allEntries.end(), entries.begin(), entries.end());
        nodeSize--;
    }
    cout << "[list-quicklist] key:" << key << " size:" << allEntries.size() << " values:";
    for (const auto& v : allEntries) {
        cout << v << " ";
    }
}

void parseHashZiplist(Buffer* buf){
    string key = read_string(buf);
    string ziplistBlob = read_string(buf);
    vector<string> entries = parseZiplistEntries(ziplistBlob);
    cout << "[hash-ziplist] key:" << key << " fields:";
    for (size_t i = 0; i + 1 < entries.size(); i += 2) {
        cout << entries[i] << "=" << entries[i + 1] << " ";
    }
}

void parseList(Buffer* buf) {
    string key = read_string(buf);
    uint64_t len = rdbLoadLen(buf);
    cout << "[list] key:" << key << " size:" << len << " values:";
    for (uint64_t i = 0; i < len; ++i) {
        cout << read_string(buf) << " ";
    }
}

void parseSet(Buffer* buf) {
    string key = read_string(buf);
    uint64_t len = rdbLoadLen(buf);
    cout << "[set] key:" << key << " size:" << len << " members:";
    for (uint64_t i = 0; i < len; ++i) {
        cout << read_string(buf) << " ";
    }
}

void parseSetIntset(Buffer* buf) {
    string key = read_string(buf);
    string blob = read_string(buf);
    const uint8_t* p = (const uint8_t*)blob.data();
    if (blob.size() < 8) {
        cout << "[set-intset] key:" << key << " invalid_blob ";
        return;
    }

    uint32_t enc = read_le32(p);
    uint32_t count = read_le32(p + 4);
    cout << "[set-intset] key:" << key << " size:" << count << " members:";
    const uint8_t* cur = p + 8;
    for (uint32_t i = 0; i < count; ++i) {
        if (enc == 2) {
            if (cur + 2 > (const uint8_t*)blob.data() + blob.size()) break;
            int16_t v = (int16_t)read_le16(cur);
            cout << v << " ";
            cur += 2;
        } else if (enc == 4) {
            if (cur + 4 > (const uint8_t*)blob.data() + blob.size()) break;
            int32_t v = (int32_t)read_le32(cur);
            cout << v << " ";
            cur += 4;
        } else if (enc == 8) {
            if (cur + 8 > (const uint8_t*)blob.data() + blob.size()) break;
            uint64_t lo = read_le32(cur);
            uint64_t hi = read_le32(cur + 4);
            int64_t v = (int64_t)((hi << 32) | lo);
            cout << v << " ";
            cur += 8;
        } else {
            break;
        }
    }
}

void parseHash(Buffer* buf) {
    string key = read_string(buf);
    uint64_t len = rdbLoadLen(buf);
    cout << "[hash] key:" << key << " fields:";
    for (uint64_t i = 0; i < len; ++i) {
        string field = read_string(buf);
        string value = read_string(buf);
        cout << field << "=" << value << " ";
    }
}