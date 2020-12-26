// lang::CwC

#pragma once

#include "string.h"

/**
 * An Object subclass representing a key that corresponds to data value in a KVStore on some node.
 * 
 * @author Spencer LaChance <lachance.s@northeastern.edu>
 * @author David Mberingabo <mberingabo.d@husky.neu.edu>
 */
class Key : public Object {
public:
    // The string key that the data will be stored at within the store
    String* key_;
    // The index of the node on which the data is stored
    size_t idx_;

    /**
     * Constructor
     */
    Key(const char* key, size_t idx) {
        key_ = new String(key);
        idx_ = idx;
    }

    /**
     * Destructor
     */
    ~Key() {
        delete key_;
    }

    /**
     * Getter for the key string. 
     */
    String* get_keystring() {
        return key_;
    }

    /**
     * Getter for the home node index.
     */
    size_t get_home_node() {
        return idx_;
    }

    /* Returns a serialized representation of this key */
    const char* serialize() {
        StrBuff buff;
        // Serialize the string
        const char* serial_k = key_->serialize();
        buff.c(serial_k);
        delete[] serial_k;
        // Serialize the node index
        const char* serial_idx = Serializer::serialize_size_t(idx_);
        buff.c(serial_idx);
        delete[] serial_idx;
        return buff.c_str();
    }

    /* Return true if this key is equal to the given objects, and false if not. */
    bool equals(Object* o) {
        Key* other = dynamic_cast<Key*>(o);
        if (other == nullptr) return false;
        return idx_ == other->get_home_node() && key_->equals(other->get_keystring());
    }

    /** Returns a copy of this Key. */
    Key* clone() { return new Key(key_->c_str(), idx_); }
};

/** 
 * Uses a StrBuff to build new keys with characters appended to an original key's string.
 * @author Jan Vitek <vitekj@me.com>
 */
class KeyBuff : public Object {
public:
    Key* orig_; // external
    StrBuff buf_;

    KeyBuff(Key* orig) : orig_(orig), buf_() {
        buf_.c(*orig_->get_keystring());
    }

    KeyBuff& c(String &s) { buf_.c(s); return *this;  }
    KeyBuff& c(size_t v) { buf_.c(v); return *this; }
    KeyBuff& c(const char* v) { buf_.c(v); return *this; }

    Key* get(size_t idx) {
        char* s = buf_.c_str();
        buf_.c(*orig_->get_keystring());
        Key* k = new Key(s, idx);
        delete[] s;
        return k;
    }
}; // KeyBuff