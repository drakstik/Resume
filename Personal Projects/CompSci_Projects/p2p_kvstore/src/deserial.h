//lang:CwC

#pragma once

#include <assert.h>
#include <sys/socket.h>
#include "message.h"
#include "datatype.h"

class DataFrame; class Column; class DistributedVector; class KVStore; class Chunk;

/**
 * Helper class that handles deserializing objects of various types.
 * 
 * @author Spencer LaChance <lachance.s@northeastern.edu>
 * @author David Mberingabo <mberingabo.d@husky.neu.edu>
 */
class Deserializer : public Object {
public:
    const char* stream_;
    int i_; // current location in the stream
    char* x_;
    

    Deserializer(const char* stream) {
        stream_ = stream;
        i_ = 0;
        x_ = new char[sizeof(char) + 1];
        x_[1] = '\0';
    }

    ~Deserializer() {
        delete[] x_;
    }

    /* Returns the current character in the stream. */
    char current() {
        return stream_[i_];
    }

    /* Return the current character in the stream and step forward by 1. */
    char step() {
        char rtrn = stream_[i_];
        i_++;
        return rtrn; 
    }

    /* Builds and returns an integer from the bytestream. */
    int deserialize_int() {
        StrBuff buff;
        assert(step() == '{');
        while (current() != '}') {
            *x_ = step();
            buff.c(x_);
        }
        assert(step() == '}');
        char* c_int = buff.c_str();
        int res = atoi(c_int);
        delete[] c_int;
        return res;
    }

    /* Builds and returns a size_t from the bytestream. */
    size_t deserialize_size_t() {
        StrBuff buff;
        assert(step() == '{');
        while (current() != '}') {
            *x_ = step();
            buff.c(x_);
        }
        assert(step() == '}');
        char* c_size_t = buff.c_str();
        size_t res = 0;
        assert(sscanf(c_size_t, "%zu", &res) == 1);
        delete[] c_size_t;
        return res;
    }

    /* Builds and returns a float from the bytestream. */
    float deserialize_float() {
        StrBuff buff;
        assert(step() == '{');
        while (current() != '}') {
            *x_ = step();
            buff.c(x_);
        }
        assert(step() == '}');
        char* c_float = buff.c_str();
        float res = atof(c_float);
        delete[] c_float;
        return res;
    }

    /* Builds and returns a boolean from the bytestream. */
    bool deserialize_bool() {
        StrBuff buff;
        assert(step() == '{');
        while (current() != '}') {
            *x_ = step();
            buff.c(x_);
        }
        assert(step() == '}');
        char* c_bool = buff.c_str();
        bool res = atoi(c_bool);
        delete[] c_bool;
        return res;
    }

    Object* deserialize_object() {
        char* serial_obj = new char[15];
        strncpy(serial_obj, stream_, 14);
        serial_obj[14] = '\0';
        assert(strcmp(serial_obj, "{type: object}") == 0);
        delete[] serial_obj;
        return new Object();
    }

    /* Builds and returns a Key from the bytestream. */
    Key* deserialize_key() {
        String* key_str = deserialize_string();
        assert(key_str != nullptr);
        size_t idx = deserialize_size_t();
        const char* key = key_str->c_str();
        Key* rtrn = new Key(key, idx);
        delete key_str;
        return rtrn;
    }

    Message* deserialize_message() {
        MsgKind kind = (MsgKind)deserialize_size_t();
        switch (kind) {
            case MsgKind::Ack:          return new Ack();
            case MsgKind::Register:     return deserialize_register();
            case MsgKind::Directory:    return deserialize_directory();
            case MsgKind::Reply:        return deserialize_reply();
            case MsgKind::Put:          return deserialize_put();
            case MsgKind::Get:          return deserialize_get();
            case MsgKind::WaitAndGet:   return deserialize_wait_get();
        }
    }

    /* Builds and returns a Directory from the bytestream. */
    Directory* deserialize_directory() {
        Vector* addresses = deserialize_string_vector();
        IntVector* indices = deserialize_int_vector();
        assert(step() == '\n');
        return new Directory(addresses, indices);
    }

    /* Builds and returns a Register from the bytestream. */
    Register* deserialize_register() {
        String* ip = deserialize_string();
        size_t sender = deserialize_size_t();
        assert(step() == '\n');
        return new Register(ip, sender);
    }

    /* Builds and returns a Put message from the bytestream. */
    Put* deserialize_put() {
        Key* k = deserialize_key();
        // Extract the blob of serialized data
        StrBuff buff;
        while (current() != '\n') {
            *x_ = step();
            buff.c(x_);
        }
        assert(step() == '\n');
        return new Put(k, buff.c_str());
    }

    /* Builds and returns a Get message from the bytestream. */
    Get* deserialize_get() {
        Key* k = deserialize_key();
        assert(step() == '\n');
        return new Get(k);
    }

    /* Builds and returns a WaitAndGet message from the bytestream. */
    WaitAndGet* deserialize_wait_get() {
        Key* k = deserialize_key();
        assert(step() == '\n');
        return new WaitAndGet(k);
    }

    /* Builds and returns a Reply message from the bytestream. */
    Reply* deserialize_reply() {
        MsgKind req = (MsgKind)deserialize_size_t();
        // Extract the serialized data
        StrBuff buff;
        while (current() != '\n') {
            *x_ = step();
            buff.c(x_);
        }
        assert(step() == '\n');
        return new Reply(buff.c_str(), req);
    }

    /* Builds and returns a String from the bytestream. */
    String* deserialize_string() {
        size_t size = deserialize_size_t();
        char* c_str = new char[size + 1];
        for (size_t i = 0; i < size; i++) {
            c_str[i] = step();
        }
        c_str[size] = '\0';
        String* res = new String(c_str);
        delete[] c_str;
        return res;
    }

    /* Builds and returns a vector from the bytestream. 
    *  Our Vector class can hold objects of all kinds, but here we are deserializing one that
    *  only holds strings.
    *  Create an vector, append strings from the stream to it and return it.
    */
    Vector* deserialize_string_vector() {
        Vector* vec = new Vector();
        size_t size = deserialize_size_t();
        for (size_t i = 0; i < size; i++) {
            String* element = deserialize_string();
            vec->append(element);
        }
        return vec;
    }

    /* Builds and returns an IntVector from the bytestream */
    IntVector* deserialize_int_vector() {
        IntVector* ivec = new IntVector();
        size_t size = deserialize_size_t();
        for (size_t i = 0; i < size; i++) {
            int element = deserialize_int();
            ivec->append(element);
        }
        return ivec;
    }

    /** Builds and returns a DataType from the bytestream. */
    DataType* deserialize_datatype() {
        char type = step();
        DataType* dt = new DataType();
        switch (type) {
            case 'I':
                dt->set_int(deserialize_int()); break;
            case 'B':
                dt->set_bool(deserialize_bool()); break;
            case 'F':
                dt->set_float(deserialize_float()); break;
            case 'S':
                dt->set_string(deserialize_string()); break;
        }
        return dt;
    }

    /** Builds and returns a Chunk from the bytestream. */
    Chunk* deserialize_chunk();

    /** Builds and returns a DistributedVector from the bytestream. */
    DistributedVector* deserialize_dist_vector(KVStore* kv);

    /** Builds and returns a Column from the bytestream. */
    Column* deserialize_column(KVStore* kv);

    /** Builds and returns a DataFrame from the bytestream. */
    DataFrame* deserialize_dataframe(KVStore* kv, Key* k);
};