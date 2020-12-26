// Lang::CwC

#include "../src/kdstore.h"

#define NROWS 10000

int main(int argc, char** argv) {
    /* Arrays to be stored in KDStore. */
    float floats[NROWS];
    bool bools[NROWS];
    int ints[NROWS];
    String* strings[NROWS];

    /* Populating the arrays. */
    for (int i = 0; i < NROWS; i++) {
        floats[i] = i + 0.1f;
        bools[i] = i % 2;
        ints[i] = i;
        strings[i] = new String("foo");
    }

    /* Scalars to be stored in KDStore. */
    float f = 1.1f;
    bool b = false;
    int i = 1;
    String s("scalar");

    KDStore* kd_ = new KDStore(0, 1);

    // /* Testing storing scalars and arrays in a KDStore. */
    Key key1("float",0);
    Key key2("bool",0);
    Key key3("int",0);
    Key key4("string",0);
    Key key5("float array",0);
    Key key6("bool array",0);
    Key key7("int array",0);
    Key key8("string array",0);

    DataFrame* df_f = DataFrame::fromFloatScalar(&key1, kd_, f);
    DataFrame* df_b = DataFrame::fromBoolScalar(&key2, kd_, b);
    DataFrame* df_i = DataFrame::fromIntScalar(&key3, kd_, i);
    DataFrame* df_s = DataFrame::fromStringScalar(&key4, kd_,  s.clone()); // clone the string because kd_ takes ownership of value

    DataFrame* df_floats = DataFrame::fromFloatArray(&key5, kd_, NROWS, floats);
    DataFrame* df_bools = DataFrame::fromBoolArray(&key6, kd_, NROWS, bools);
    DataFrame* df_ints = DataFrame::fromIntArray(&key7, kd_, NROWS, ints);
    DataFrame* df_strings = DataFrame::fromStringArray(&key8, kd_, NROWS, strings);

    /* Testing if the same values are contained in the dataframe
     * and if the dataframe has the right amount of columns and rows. */
    assert(df_f->ncols() == 1);
    assert(df_f->nrows() == 1);
    float f_ = df_f->get_float(0, 0);
    assert(f_ == f);

    assert(df_i->ncols() == 1);
    assert(df_i->nrows() == 1);
    int i_ = df_i->get_int(0, 0);
    assert(i_ == i);

    assert(df_b->ncols() == 1);
    assert(df_b->nrows() == 1);
    bool b_ = df_b->get_bool(0, 0);
    assert(b_ == b);

    assert(df_s->ncols() == 1);
    assert(df_s->nrows() == 1);
    String* s_ = df_s->get_string(0, 0);
    assert(s_->equals(&s));

    assert(df_floats->ncols() == 1);
    assert(df_floats->nrows() == NROWS);
    for (int i = 0; i > NROWS; i++) {
        assert(df_floats->get_float(0, i) == floats[i]);
    }

    assert(df_bools->ncols() == 1);
    assert(df_bools->nrows() == NROWS);
    for (int i = 0; i > NROWS; i++) {
        assert(df_bools->get_bool(0, i) == bools[i]);
    }

    assert(df_ints->ncols() == 1);
    assert(df_ints->nrows() == NROWS);
    for (int i = 0; i > NROWS; i++) {
        assert(df_ints->get_int(0, i) == ints[i]);
    }

    assert(df_strings->ncols() == 1);
    assert(df_strings->nrows() == NROWS);
    for (int i = 0; i > NROWS; i++) {
        assert(df_strings->get_string(0, i) == strings[i]);
    }

    /* Testing get() method in KDStore. */
    DataFrame* get_df1 = kd_->get(key1);
    assert(get_df1->equals(df_f));
    delete get_df1;

    DataFrame* get_df2 = kd_->get(key2);
    assert(get_df2->equals(df_b));
    delete get_df2;

    DataFrame* get_df3 = kd_->get(key3);
    assert(get_df3->equals(df_i));
    delete get_df3;

    DataFrame* get_df4 = kd_->get(key4);
    assert(get_df4->equals(df_s));
    delete get_df4;

    DataFrame* get_df5 = kd_->get(key5);
    assert(get_df5->equals(df_floats));
    delete get_df5;

    DataFrame* get_df6 = kd_->get(key6);
    assert(get_df6->equals(df_bools));
    delete get_df6;

    DataFrame* get_df7 = kd_->get(key7);
    assert(get_df7->equals(df_ints));
    delete get_df7;

    DataFrame* get_df8 = kd_->get(key8);
    assert(get_df8->equals(df_strings));
    delete get_df8;

    KVStore* kv_ = kd_->get_kv();

    /* Testing get() method in KVStore. */
    const char* serial_df_f1 = kv_->get(key1);
    const char* serial_df_f2 = df_f->serialize();
    assert(strcmp(serial_df_f1, serial_df_f2) == 0);
    delete[] serial_df_f1; delete[] serial_df_f2;

    const char* serial_df_b1 = kv_->get(key2);
    const char* serial_df_b2 = df_b->serialize();
    assert(strcmp(serial_df_b1, serial_df_b2) == 0);
    delete[] serial_df_b1; delete[] serial_df_b2;

    const char* serial_df_i1 = kv_->get(key3);
    const char* serial_df_i2 = df_i->serialize();
    assert(strcmp(serial_df_i1, serial_df_i2) == 0);
    delete[] serial_df_i1; delete[] serial_df_i2;

    const char* serial_df_s1 = kv_->get(key4);
    const char* serial_df_s2 = df_s->serialize();
    assert(strcmp(serial_df_s1, serial_df_s2) == 0);
    delete[] serial_df_s1; delete[] serial_df_s2;

    const char* serial_df_floats1 = kv_->get(key5);
    const char* serial_df_floats2 = df_floats->serialize();
    assert(strcmp(serial_df_floats1, serial_df_floats2) == 0);
    delete[] serial_df_floats1; delete[] serial_df_floats2;

    const char* serial_df_bools1 = kv_->get(key6);
    const char* serial_df_bools2 = df_bools->serialize();
    assert(strcmp(serial_df_bools1, serial_df_bools2) == 0);
    delete[] serial_df_bools1; delete[] serial_df_bools2;

    const char* serial_df_ints1 = kv_->get(key7);
    const char* serial_df_ints2 = df_ints->serialize();
    assert(strcmp(serial_df_ints1, serial_df_ints2) == 0);
    delete[] serial_df_ints1; delete[] serial_df_ints2;

    const char* serial_df_strings1 = kv_->get(key8);
    const char* serial_df_strings2 = df_strings->serialize();
    assert(strcmp(serial_df_strings1, serial_df_strings2) == 0);
    delete[] serial_df_strings1; delete[] serial_df_strings2;

    kd_->done();
    delete kd_;
    delete df_f; delete df_i; delete df_b; delete df_s; 
    delete df_floats; delete df_bools; delete df_ints; delete df_strings;
    delete s_;

    Sys sys;
    s.pln("kvstore test was SUCCESSFUL");
    return 0;
}