#include <iostream>
#include <fstream>
#include <cstdlib>
#include <cstdint>
#include <string>
#include <cstring>
#include  "rdb.h"
#include  "ziplist.h"
#include "endianconv.h"
#include "assert.h"
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

string read_string(Buffer* buf) {
    uint8_t type = buffer_read_byte(buf);

    if ((type & 0xC0) == 0x00) {
        int len = type & 0x3F;
        string s(len, 0);
        buffer_read_bytes(buf, (uint8_t*)&s[0], len);
        return s;
    }
    else if ((type & 0xC0) == 0x40) {
        int len = ((type & 0x3F) << 8) | buffer_read_byte(buf);
        string s(len, 0);
        buffer_read_bytes(buf, (uint8_t*)&s[0], len);
        return s;
    }
    else if ((type & 0xC0) == 0x80) {
        uint32_t len = buffer_read_u32(buf);
        string s(len, 0);
        buffer_read_bytes(buf, (uint8_t*)&s[0], len);
        return s;
    }
    else {
        int fmt = type & 0x3F;
        long long num;

        if (fmt == 0) num = buffer_read_byte(buf);
        else if (fmt == 1) num = (buffer_read_byte(buf) << 8) | buffer_read_byte(buf);
        else if (fmt == 2) num = buffer_read_u32(buf);
        else if (fmt == 3) num = ((long long)buffer_read_u32(buf) << 32) | buffer_read_u32(buf);
        else return "unknown_int";

        return to_string(num);
    }
}

// 读取 Redis 长度编码（对应 rdbSaveLen）
// 返回：读到的长度
uint64_t rdbLoadLen(Buffer* buf) {
    uint8_t b = buffer_read_byte(buf);

    // 最高2位表示类型
    int type = (b >> 6) & 0x03;

    uint64_t len;

    if (type == 0) {
        // 00xxxxxx  6位长度
        len = b & 0x3F;
    }
    else if (type == 1) {
        // 01xxxxxx  14位长度
        len = (b & 0x3F) << 8;
        len |= buffer_read_byte(buf);
    }
    else if (type == 2) {
        // 10000000  32位长度
        len = buffer_read_u32(buf);
    }
    else if (type == 3) {
        // 11000000  64位长度
        uint64_t biglen = 0;
        biglen |= (uint64_t)buffer_read_byte(buf) << 56;
        biglen |= (uint64_t)buffer_read_byte(buf) << 48;
        biglen |= (uint64_t)buffer_read_byte(buf) << 40;
        biglen |= (uint64_t)buffer_read_byte(buf) << 32;
        biglen |= (uint64_t)buffer_read_byte(buf) << 24;
        biglen |= (uint64_t)buffer_read_byte(buf) << 16;
        biglen |= (uint64_t)buffer_read_byte(buf) << 8;
        biglen |= (uint64_t)buffer_read_byte(buf);
        len = biglen;
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

void loadDb(Buffer* buf);

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
            loadDb(&buf);
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

void rdbLoadInfoAuxFields(Buffer* buff) {
    string aux_key = read_string(buff);
    string aux_val = read_string(buff);
    cout << "[AUX] " << aux_key << " = " << aux_val << endl;
}

void parseString(Buffer* buf);

void loadDb(Buffer* buf) {
    uint64_t dbNum = rdbLoadLen(buf);
    cout << "select db " << dbNum << endl;
    uint8_t op = buffer_read_byte(buf);
    uint64_t db_size, expires_size;
    db_size = rdbLoadLen(buf);
    expires_size = rdbLoadLen(buf);
    cout << "RDB_OPCODE " << (int)op << " db_size " << db_size << " expires_size " << expires_size << endl;
    uint8_t type;
    while (true) {
        type = buffer_read_byte(buf);
        long long expiretime = 0;
        uint64_t idletime = 0;
        uint8_t lfu_freq = 0;
        if (type == RDB_OPCODE_EXPIRETIME_MS) {
            expiretime = rdbLoadMillisecondTime(buf);
        }
        else if (type == RDB_OPCODE_IDLE) {
            idletime = rdbLoadLen(buf) * 1000;
        }
        else if (type == RDB_OPCODE_FREQ) {
            lfu_freq = rdbLoadLFUFreq(buf);
        }
        else if (type == RDB_TYPE_STRING) {
            parseString(buf);
        }
        else if (type == RDB_OPCODE_EOF) {
            break;
        }
        else {
            cout << "暂不支持的数据类型：" << (int)type << endl;
            break;
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
}

void parseString(Buffer* buf) {
    string key = read_string(buf);
    string value = read_string(buf);
    cout << "[string] " << "key:" << key << " value:" << value << " ";
}

void parseZiplist(Buffer* buf){
    size_t len = rdbLoadLen(buf);
    uint8_t* data = new uint8_t[len];
    buffer_read_bytes(buf,data,len);
    const uint8_t* p = data;
    uint32_t zlbytes = read_le32(p);
    p+=4;
    uint32_t zltail_offset = read_le32(p);
    p+=4;
    uint16_t zllen = read_le16(p);
    p+=2;
    int entrySize = 0;
    while (*p!=ZIP_END){
        unsigned int prevlensize, prevlen = 0;
        ZIP_DECODE_PREVLEN(p, prevlensize, prevlen);
    }
}