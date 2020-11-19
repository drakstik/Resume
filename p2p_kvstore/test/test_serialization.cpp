// //lang::CwC

#include <assert.h>
#include "../src/deserial.h"
#include "../src/dataframe.h"

#define NROWS 10000

/* Utility method for creating a DataFrame with foo values. */
DataFrame* df_(KVStore* kv, Key* k) {
    String s1("hi");
    String s2("bye");
    String s3("hi");
    String s4("bye");

    Schema s("IBFS");
    DataFrame* df = new DataFrame(s, kv, k);
    Row r(s);
    for (int i = 1; i <= NROWS / 4; i++) {
        r.set(0, 1);
        r.set(1, true);
        r.set(2, 1.1f);
        r.set(3, s1.clone());
        df->add_row(r, false);
        
        r.set(0, 2);
        r.set(1, false);
        r.set(2, 2.2f);
        r.set(3, s2.clone());
        df->add_row(r, false);
        
        r.set(0, 3);
        r.set(1, true);
        r.set(2, 3.3f);
        r.set(3, s3.clone());
        df->add_row(r, false);
        
        r.set(0, 4);
        r.set(1, false);
        r.set(2, 4.4f);
        r.set(3, s4.clone());
        if (i == NROWS / 4) df->add_row(r, true);
        else df->add_row(r, false);
    }

    return df;
}

/** Testing IntVector serialization and deserialization. */
void test_int_vector_serialization() {
    IntVector* ivec = new IntVector();
    ivec->append(1);
    ivec->append(2); 
    ivec->append(3); 
    ivec->append(4); 
    ivec->append(5); 

    // IntVector serialization
    const char* serialized_ivec = ivec->serialize();
    
    // IntVector deserialization
    Deserializer ivec_ds(serialized_ivec);
    IntVector* deserialized_ivec = ivec_ds.deserialize_int_vector();
    assert(deserialized_ivec != nullptr);
    assert(deserialized_ivec->equals(ivec));

    delete ivec;
    delete[] serialized_ivec;
    delete deserialized_ivec;
}

/* Testings Vector serialization and deserialization */
void test_string_vector_serialization() {
    String* s1 = new String("abc");
    String* s2 = new String("abcd");
    String* s3 = new String("abcdefghi");
    String* s4 = new String("abcdefghij");

    // String serialization and deserialization 
    const char* serialized_str = s1->serialize();
    Deserializer string_ds(serialized_str);
    String* deserialized_str = string_ds.deserialize_string();
    assert(deserialized_str != nullptr);
    assert(s1->equals(deserialized_str));

    Vector* vec = new Vector();
    vec->append(s1);
    vec->append(s2);
    vec->append(s3);
    vec->append(s4);

    /* Vector serialization */
    const char* serialized_vec = vec->serialize();
    
    /* Vector desserialization */
    Deserializer svec_ds(serialized_vec);
    Vector* deserialized_vec = svec_ds.deserialize_string_vector();
    assert(deserialized_vec != nullptr);
    assert(deserialized_vec->equals(vec));

    
    delete[] serialized_str;
    delete[] serialized_vec;
    delete vec;
    delete deserialized_str;
    delete deserialized_vec;
}

void test_message_serialization(KVStore* kv) {
    /* Ack construction */
    Ack* ack = new Ack();

    /* Ack serialization */
    const char* serialized_ack = ack->serialize();

    /* Ack deserialization */
    Deserializer ack_ds(serialized_ack);
    Ack* deserialized_ack = ack_ds.deserialize_message()->as_ack();
    assert(deserialized_ack != nullptr);
    assert(deserialized_ack->equals(ack)); // Testing acknowledge equality.

    delete ack;
    delete[] serialized_ack;
    delete deserialized_ack;

    /* Directory construction */
    Directory* dir = new Directory();
    dir->add_client("0.0.0.0", 0);
    dir->add_client("0.1.0.0", 1);
    dir->add_client("0.2.0.0", 2);
    dir->add_client("0.3.0.0", 3);

    /* Directory serialization */
    const char* serialized_directory = dir->serialize();

    /* Directory deserialization */
    Deserializer directory_ds(serialized_directory);
    Directory* deserialized_directory = directory_ds.deserialize_message()->as_directory();
    assert(deserialized_directory != nullptr);
    assert(deserialized_directory->equals(dir)); // Testing directory equality

    delete dir;
    delete[] serialized_directory;
    delete deserialized_directory;

    /* Register construction */
    String* ip = new String("127.0.0.3");
    Register* reg = new Register(ip, 1);

    /* Register serialization */
    const char* serialized_register = reg->serialize();

    /* Register deserialization */
    Deserializer register_ds(serialized_register);
    Register* deserialized_register = register_ds.deserialize_message()->as_register();
    assert(deserialized_register != nullptr);
    assert(deserialized_register->equals(reg));

    delete reg;
    delete[] serialized_register;
    delete deserialized_register;

    /* Put construction */
    Key* key1 = new Key("foo",0);
    DataFrame* df = df_(kv, key1);
    const char* serial_df = df->serialize();
    Put* put = new Put(key1, serial_df);

    /* Put serialization */
    const char* serialized_put = put->serialize();

    /* Put deserialization */
    Deserializer put_deserializer(serialized_put);
    Put* deserialized_put = put_deserializer.deserialize_message()->as_put();
    assert(deserialized_put != nullptr);
    assert(deserialized_put->equals(put));

    delete[] serial_df;
    delete put;
    delete[] serialized_put;
    delete[] deserialized_put->get_value();
    delete deserialized_put->get_key();
    delete deserialized_put;

    /* Get construction */
    Key* key2 = new Key("foo", 0);
    Get* get = new Get(key2);

    /* Get serialization */
    const char* serialized_get = get->serialize();

    /* Get deserialization */
    Deserializer get_deserializer(serialized_get);
    Get* deserialized_get = get_deserializer.deserialize_message()->as_get();
    assert(deserialized_get != nullptr);
    assert(deserialized_get->equals(get));

    delete key2;
    delete get;
    delete[] serialized_get;
    delete deserialized_get->get_key();
    delete deserialized_get;

    /* WaitAndGet construction */
    Key* key3 = new Key("foo", 0);
    WaitAndGet* w_get = new WaitAndGet(key3);

    /* WaitAndGet serialization */
    const char* serialized_w_get = w_get->serialize();

    /* WaitAndGet deserialization */
    Deserializer w_get_deserializer(serialized_w_get);
    WaitAndGet* deserialized_w_get = w_get_deserializer.deserialize_message()->as_wait_and_get();
    assert(deserialized_w_get != nullptr);
    assert(deserialized_w_get->equals(w_get));

    delete key3;
    delete w_get;
    delete[] serialized_w_get;
    delete deserialized_w_get->get_key();
    delete deserialized_w_get;

    /* Reply construction */
    const char* serial_df2 = df->serialize();
    Reply* rep = new Reply(serial_df2, MsgKind::Get);

    /* Reply serialization */
    const char* serialized_reply = rep->serialize();

    /* Reply deserialization */
    Deserializer reply_deserializer(serialized_reply);
    Reply* deserialized_reply = reply_deserializer.deserialize_message()->as_reply();
    assert(deserialized_reply != nullptr);
    assert(deserialized_reply->equals(rep));

    delete key1;
    delete df;
    delete[] serial_df2;
    delete rep;
    delete[] serialized_reply;
    delete[] deserialized_reply->get_value();
    delete deserialized_reply;
}

void test_object_serialization() {
    Object* o = new Object();
    const char* serialized_object = o->serialize();

    Deserializer object_ds(serialized_object);
    // the assert statement within deserialize_object() proves that this test passes
    Object* deserialized_object = object_ds.deserialize_object();

    delete o;
    delete deserialized_object;
}

void test_dataframe_serialization(KVStore* kv) {
    Key* k = new Key("df", 0);
    KeyBuff kbuf(k);
    /* Testing BoolColumn serialization and deserialization. */
    kbuf.c("-c4");
    Column* bcol = new Column('B', kv, kbuf.get(0), 4,true,false,true,false);
    const char* serialized_bcol = bcol->serialize();
    Deserializer bcol_ds(serialized_bcol);
    Column* deserialized_bcol = bcol_ds.deserialize_column(kv);
    assert(deserialized_bcol != nullptr);
    assert(deserialized_bcol->equals(bcol));

    /* Testing IntColumn serialization and deserialization. */
    kbuf.c("-c5");
    Column* icol = new Column('I', kv, kbuf.get(0), 4,1,2,3,4);
    const char* serialized_icol = icol->serialize();
    Deserializer icol_ds(serialized_icol);
    Column* deserialized_icol = icol_ds.deserialize_column(kv);
    assert(deserialized_icol != nullptr);
    assert(deserialized_icol->equals(icol));

    /* Testing FloatColumn serialization and deserialization. */
    kbuf.c("-c6");
    Column* fcol = new Column('F', kv, kbuf.get(0), 4,1.1f,2.2f,3.3f,4.4f);
    const char* serialized_fcol = fcol->serialize();
    Deserializer fcol_ds(serialized_fcol);
    Column* deserialized_fcol = fcol_ds.deserialize_column(kv);
    assert(deserialized_fcol != nullptr);
    assert(deserialized_fcol->equals(fcol));

    /* Testing StringColumn serialization and deserialization. */
    String* s1 = new String("hi");
    String* s2 = new String("bye");
    String* s3 = new String("hi");
    String* s4 = new String("bye");
    kbuf.c("-c7");
    Column* scol = new Column('S', kv, kbuf.get(0), 4,s1,s2,s3,s4);
    const char* serialized_scol = scol->serialize();
    Deserializer scol_ds(serialized_scol);
    Column* deserialized_scol = scol_ds.deserialize_column(kv);
    assert(deserialized_scol != nullptr);
    assert(deserialized_scol->equals(scol));

    /* Testing DataFrame serialization and deserialization. */
    DataFrame* df = df_(kv, k);
    df->add_column(icol);
    df->add_column(bcol);
    df->add_column(fcol);
    df->add_column(scol);
    const char* serialized_df = df->serialize();
    Deserializer df_ds(serialized_df);
    DataFrame* deserialized_df = df_ds.deserialize_dataframe(kv, k);
    assert(deserialized_df != nullptr);
    assert(deserialized_df->equals(df));

    delete k;
    delete df;
    delete deserialized_df;
    delete deserialized_icol;
    delete deserialized_bcol;
    delete deserialized_fcol;
    delete deserialized_scol;
    delete[] serialized_icol;
    delete[] serialized_bcol;
    delete[] serialized_fcol;
    delete[] serialized_scol;
    delete[] serialized_df;
}

void test_key_serialization() {
    /* Construct a key */
    Key* k = new Key("Key 1", 0);

    /* Key serialization */
    const char* serialized_key = k->serialize();

    /* Key deserialization */
    Deserializer key_deserializer(serialized_key);
    Key* deserialized_key = key_deserializer.deserialize_key();
    assert(deserialized_key != nullptr);
    assert(deserialized_key->equals(k));

    delete k;
    delete[] serialized_key;
    delete deserialized_key;
    
}

int main() {
    KVStore* kv = new KVStore(0, 1);

    test_object_serialization();
    test_int_vector_serialization();
    test_string_vector_serialization();
    test_key_serialization();
    test_dataframe_serialization(kv);
    test_message_serialization(kv);
    printf("All serialization tests passed!\n");
    
    kv->shutdown();
    delete kv;
    return 0;
}