//lang::CwC

#pragma once

#include "parser_main.h"

/**
 * This class is a middle man between the Applications which give and receive DataFrames and the
 * KVStore which gives and receives blobs of serialized data.
 * 
 * @author Spencer LaChance <lachance.s@northeastern.edu>
 * @author David Mberingabo <mberingabo.d@husky.neu.edu>
 */
class KDStore : public Object {
public:
    KVStore kv_;

    KDStore(size_t idx, size_t nodes) : kv_(idx, nodes) { }

    /** Gets the DataFrame stored at the given key in the KVStore. */
    DataFrame* get(Key& k) {
        const char* serialized_df = kv_.get(k);
        Deserializer ds(serialized_df);
        DataFrame* res = ds.deserialize_dataframe(&kv_, &k);
        delete[] serialized_df;
        return res;
    }

    /** Waits until the given key is put into the KVStore and then retrives its value. */
    DataFrame* wait_and_get(Key& k) {
        const char* serialized_df = kv_.wait_and_get(k);
        Deserializer ds(serialized_df);
        DataFrame* res = ds.deserialize_dataframe(&kv_, &k);
        delete[] serialized_df;
        return res;
    }

    /** Getter for this KDStore's associated KVStore. */
    KVStore* get_kv() { return &kv_; }

    /** Called when the application has finished its execution. */
    void done() { kv_.shutdown(); }
};

// DataFrame functions defined below to avoid circular dependencies

/**
 * Builds a DataFrame from rows created by the given visitor, adds the DataFrame to the
 * given KDStore at the given Key, and then returns the DataFrame.
 */
DataFrame* DataFrame::fromVisitor(Key* k, KDStore* kd, const char* scm, Writer& w) {
    Schema schema(scm);
    KVStore* kv = kd->get_kv();
    DataFrame* res = new DataFrame(schema, kv, k);
    Row r(schema);
    while (!w.done()) {
        w.visit(r);
        res->add_row(r, false);
    }
    res->lock_columns();
    kv->put(*k, res->serialize());
    return res;
}

/**
 * Builds a DataFrame from an input SoR file, adds the DataFrame to the given KDStore at the 
 * given Key, and then returns the DataFrame.
 */
DataFrame* DataFrame::fromFile(const char* filename, Key* k, KDStore* kd, char* len) {
    KVStore* kv = kd->get_kv();
    // Spoof the command line arguments that ParserMain expects
    size_t argc = 5;
    const char** argv = new const char*[argc];
    argv[0] = "foo";
    argv[1] = "-f";
    argv[2] = filename;
    argv[3] = "-len";
    argv[4] = len;

    ParserMain pf(argc, argv, kv, k);
    DataFrame* res = pf.get_dataframe();
    kv->put(*k, res->serialize());
    delete[] argv;
    return res;
}

/**
 * Builds a DataFrame with one column containing the data in the given int array, adds the
 * DataFrame to the given KDStore at the given Key, and then returns the DataFrame.
 */
DataFrame* DataFrame::fromIntArray(Key* k, KDStore* kd, size_t size, int* vals) {
    KVStore* kv = kd->get_kv();
    KeyBuff kbuf(k);
    kbuf.c("-c0");
    Column* col = new Column('I', kv, kbuf.get(kv->this_node()));
    for (int i = 0; i < size; i++) {
        col->push_back(vals[i]);
    }
    col->lock();
    DataFrame* res = new DataFrame(kv, k);
    res->add_column(col);
    kv->put(*k, res->serialize());
    return res;
}

/**
 * Builds a DataFrame with one column containing the data in the given bool array, adds the
 * DataFrame to the given KDStore at the given Key, and then returns the DataFrame.
 */
DataFrame* DataFrame::fromBoolArray(Key* k, KDStore* kd, size_t size, bool* vals) {
    KVStore* kv = kd->get_kv();
    KeyBuff kbuf(k);
    kbuf.c("-c0");
    Column* col = new Column('B', kv, kbuf.get(kv->this_node()));
    for (int i = 0; i < size; i++) {
        col->push_back(vals[i]);
    }
    col->lock();
    DataFrame* res = new DataFrame(kv, k);
    res->add_column(col);
    kv->put(*k, res->serialize());
    return res;
}

/**
 * Builds a DataFrame with one column containing the data in the given float array, adds the
 * DataFrame to the given KDStore at the given Key, and then returns the DataFrame.
 */
DataFrame* DataFrame::fromFloatArray(Key* k, KDStore* kd, size_t size, float* vals) {
    KVStore* kv = kd->get_kv();
    KeyBuff kbuf(k);
    kbuf.c("-c0");
    Column* col = new Column('F', kv, kbuf.get(kv->this_node()));
    for (int i = 0; i < size; i++) {
        col->push_back(vals[i]);
    }
    col->lock();
    DataFrame* res = new DataFrame(kv, k);
    res->add_column(col);
    kv->put(*k, res->serialize());
    return res;
}

/**
 * Builds a DataFrame with one column containing the data in the given string array, adds
 * the DataFrame to the given KDStore at the given Key, and then returns the DataFrame.
 */
DataFrame* DataFrame::fromStringArray(Key* k, KDStore* kd, size_t size, String** vals) {
    KVStore* kv = kd->get_kv();
    KeyBuff kbuf(k);
    kbuf.c("-c0");
    Column* col = new Column('S', kv, kbuf.get(kv->this_node()));
    for (int i = 0; i < size; i++) {
        col->push_back(vals[i]);
    }
    col->lock();
    DataFrame* res = new DataFrame(kv, k);
    res->add_column(col);
    kv->put(*k, res->serialize());
    return res;
}

/**
 * Builds a DataFrame with one column containing the single given int, adds the DataFrame
 * to the given KDStore at the given Key, and then returns the DataFrame.
 */
DataFrame* DataFrame::fromIntScalar(Key* k, KDStore* kd, int val) {
    KVStore* kv = kd->get_kv();
    KeyBuff kbuf(k);
    kbuf.c("-c0");
    Column* col = new Column('I', kv, kbuf.get(kv->this_node()));
    col->push_back(val);
    col->lock();
    DataFrame* res = new DataFrame(kv, k);
    res->add_column(col);
    kv->put(*k, res->serialize());
    return res;
}

/**
 * Builds a DataFrame with one column containing the single given bool, adds the DataFrame
 * to the given KDStore at the given Key, and then returns the DataFrame.
 */
DataFrame* DataFrame::fromBoolScalar(Key* k, KDStore* kd, bool val) {
    KVStore* kv = kd->get_kv();
    KeyBuff kbuf(k);
    kbuf.c("-c0");
    Column* col = new Column('B', kv, kbuf.get(kv->this_node()));
    col->push_back(val);
    col->lock();
    DataFrame* res = new DataFrame(kv, k);
    res->add_column(col);
    kv->put(*k, res->serialize());
    return res;
}

/**
 * Builds a DataFrame with one column containing the single given float, adds the DataFrame
 * to the given KDStore at the given Key, and then returns the DataFrame.
 */
DataFrame* DataFrame::fromFloatScalar(Key* k, KDStore* kd, float val) {
    KVStore* kv = kd->get_kv();
    KeyBuff kbuf(k);
    kbuf.c("-c0");
    Column* col = new Column('F', kv, kbuf.get(kv->this_node()));
    col->push_back(val);
    col->lock();
    DataFrame* res = new DataFrame(kv, k);
    res->add_column(col);
    kv->put(*k, res->serialize());
    return res;
}

/**
 * Builds a DataFrame with one column containing the single given string, adds the DataFrame
 * to the given KDStore at the given Key, and then returns the DataFrame.
 */
DataFrame* DataFrame::fromStringScalar(Key* k, KDStore* kd, String* val) {
    KVStore* kv = kd->get_kv();
    KeyBuff kbuf(k);
    kbuf.c("-c0");
    Column* col = new Column('S', kv, kbuf.get(kv->this_node()));
    col->push_back(val);
    col->lock();
    DataFrame* res = new DataFrame(kv, k);
    res->add_column(col);
    kv->put(*k, res->serialize());
    return res;
}