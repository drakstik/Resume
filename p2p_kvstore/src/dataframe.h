// //lang::CwC + a little Cpp

#pragma once

#include <thread>

#include "vector.h"
#include "helper.h"
#include "schema.h"
#include "column.h"
#include "row.h"

class KDStore;
class Key;

/**
 * Fielder that prints each field.
 * 
 * @author David Mberingabo <mberingabo.d@husky.neu.edu>
 * @author Spencer LaChance <lachance.s@northeastern.edu>
 */
class PrintFielder : public Fielder {
public:
    void start(size_t r) { }
    void accept(bool b) { printf("<%d>", b); }
    void accept(float f) { printf("<%f>", f); }
    void accept(int i) { printf("<%d>", i); }
    void accept(String* s) { printf("<%s>", s->c_str()); }
    void done() { }
};

/**
 * Rower that prints each row.
 * 
 * @author David Mberingabo <mberingabo.d@husky.neu.edu>
 * @author Spencer LaChance <lachance.s@northeastern.edu>
 */
class PrintRower : public Rower {
public:
    PrintFielder* pf_;

    PrintRower() {
        pf_ = new PrintFielder();
    }

    ~PrintRower() {
        delete pf_;
    }

    bool accept(Row& r) { 
        r.visit(r.get_idx(), *pf_);
        printf("\n");
    }

    void join_delete(Rower* other) { }
};

/****************************************************************************
 * DataFrame::
 *
 * A DataFrame is table composed of columns of equal length. Each column
 * holds values of the same type (I, S, B, F). A dataframe has a schema that
 * describes it.
 * 
 * @author Spencer LaChance <lachance.s@northeastern.edu>
 * @author David Mberingabo <mberingabo.d@husky.neu.edu>
 */
class DataFrame : public Object {
public:
    Vector columns_;
    Schema schema_;
    // Number of rows
    size_t length_;
    // The current node's KVStore, external
    KVStore* kv_;
    // The key that this DataFrame is stored at, external
    Key* k_;
    
    /** Create a data frame from a schema and columns. All columns are created empty. */
    DataFrame(Schema& schema, KVStore* kv, Key* k) : 
        schema_(schema), length_(0), kv_(kv), k_(k) {
        IntVector* types = schema.get_types();
        KeyBuff kbuf(k_);
        for (int i = 0; i < types->size(); i++) {
            // Build the column's key and then use that, the type, and the KVStore to
            // instantiate it
            kbuf.c("-c");
            kbuf.c(i);
            columns_.append(new Column(types->get(i), kv_, kbuf.get(kv->this_node())));
        }
    }

    /**
     * Creates an empty DataFrame with an empty schema. The intended use for this constructor
     * is the case where columns will be added to the DataFrame. Then, as each column is added,
     * its type is added to the schema.
     */
    DataFrame(KVStore* kv, Key* k) : kv_(kv), k_(k), length_(0) { }
    
    /** Returns the dataframe's schema. Modifying the schema after a dataframe
         * has been created in undefined. */
    Schema& get_schema() { return schema_; }
    
    /** Adds a column this dataframe, updates the schema, the new column
         * is external, and appears as the last column of the dataframe. 
         * A nullptr column is undefined. */
    void add_column(Column* col) {
        exit_if_not(col != nullptr, "Undefined column provided.");
        if (col->size() < length_) {
            pad_column_(col);
        } else if (col->size() > length_) {
            length_ = col->size();
            for (int i = 0; i < columns_.size(); i++) {
                pad_column_(dynamic_cast<Column*>(columns_.get(i)));
            }
        }
        columns_.append(col);
        if (columns_.size() > schema_.width()) {
            // This column is not in the schema, so add it
            schema_.add_column(col->get_type());
        }
    }
    
    /** Return the value at the given column and row. Accessing rows or
     *  columns out of bounds, or request the wrong type is undefined.*/
    int get_int(size_t col, size_t row) {
        Column* column = dynamic_cast<Column*>(columns_.get(col));
        return column->get_int(row);
    }
    bool get_bool(size_t col, size_t row) {
        Column* column = dynamic_cast<Column*>(columns_.get(col));
        return column->get_bool(row);
    }
    float get_float(size_t col, size_t row) {
        Column* column = dynamic_cast<Column*>(columns_.get(col));
        return column->get_float(row);
    }
    String* get_string(size_t col, size_t row) {
        Column* column = dynamic_cast<Column*>(columns_.get(col));
        return column->get_string(row);
    }

    /** Returns the index of the node on which the field at the given row idx is stored. */
    size_t get_node(size_t row) {
        Column* column = dynamic_cast<Column*>(columns_.get(0));
        return column->get_node(row);
    }
    
    /** Set the fields of the given row object with values from the columns at
         * the given offset.  If the row is not form the same schema as the
         * dataframe, results are undefined. */
    void fill_row(size_t idx, Row& row) {
        exit_if_not(schema_.get_types()->equals(row.get_types()), 
            "Row's schema does not match the data frame's.");
        for (int j = 0; j < ncols(); j++) {
            Column* col = dynamic_cast<Column*>(columns_.get(j));
            char type = col->get_type();
            switch (type) {
                case 'I':
                    row.set(j, col->get_int(idx));
                    break;
                case 'B':
                    row.set(j, col->get_bool(idx));
                    break;
                case 'F':
                    row.set(j, col->get_float(idx));
                    break;
                case 'S':
                    row.set(j, col->get_string(idx));
                    break;
            }
        }
    }
    
    /** Add a row at the end of this dataframe. The row is expected to have
         * the right schema and be filled with values, otherwise undefined.  */
    void add_row(Row& row, bool last_row) {
        exit_if_not(schema_.get_types()->equals(row.get_types()), 
            "Row's schema does not match the data frame's.");
        for (int j = 0; j < ncols(); j++) {
            Column* col = dynamic_cast<Column*>(columns_.get(j));
            char type = col->get_type();
            switch (type) {
                case 'I':
                    col->push_back(row.get_int(j));
                    break;
                case 'B':
                    col->push_back(row.get_bool(j));
                    break;
                case 'F':
                    col->push_back(row.get_float(j));
                    break;
                case 'S':
                    // Clone the string so that the Column and Row can both maintain control
                    // of their string objects.
                    col->push_back(row.get_string(j)->clone());
                    break;
                default:
                    exit_if_not(false, "Column has invalid type.");
            }
            // Finalize the columns
            if (last_row) col->lock();
        }
        length_++;
    }
    
    /** The number of rows in the dataframe. */
    size_t nrows() { return length_; }
    
    /** The number of columns in the dataframe.*/
    size_t ncols() { return columns_.size(); }
    
    /** Visit rows in order */
    void map(Rower& r) {
        Row row(schema_);
        for (int i = 0; i < length_; i++) {
            row.set_idx(i);
            fill_row(i, row);
            r.accept(row);
        }
    }

    /** Visit only the rows that are stored on the current node */
    void local_map(Rower& r) {
        Row row(schema_);
        for (int i = 0; i < length_; i++) {
            if (get_node(i) == kv_->this_node()) {
                row.set_idx(i);
                fill_row(i, row);
                r.accept(row);
            }
        }
    }

    /** Create a new dataframe, constructed from rows for which the given Rower
         * returned true from its accept method. */
    DataFrame* filter(Rower& r) {
        DataFrame* df = new DataFrame(schema_, kv_, k_);
        for (int i = 0; i < length_; i++) {
            Row row(schema_);
            fill_row(i, row);
            if (r.accept(row)) {
                df->add_row(row, false);
            }
        }
        df->lock_columns();
        return df;
    }

    /** Locks all of this DataFrame's columns. */
    void lock_columns() {
        for (int j = 0; j < ncols(); j++)
            dynamic_cast<Column*>(columns_.get(j))->lock();
    }
    
    /** Print the dataframe in SoR format to standard output. */
    void print() {
        PrintRower pr;
        map(pr);
        printf("\n");
    }

    /** Getter for the dataframe's columns. */
    Vector* get_columns() { return &columns_; }

    /** Pads the given column with a default value until its length
     *  matches the number of rows in the data frame. */
    void pad_column_(Column* col) {
        col->unlock();
        char type = col->get_type();
        while (col->size() < length_) {
            switch(type) {
                case 'I':
                    col->append_missing();
                    break;
                case 'B':
                    col->append_missing();
                    break;
                case 'F':
                    col->append_missing();
                    break;
                case 'S':
                    col->append_missing();
                    break;
                default:
                    exit_if_not(false, "Invalid column type.");
            }
        }
        col->lock();
    }

    /* Returns a serialized representation of this DataFrame */
    const char* serialize() {
        StrBuff buff;
        // Serialize the columns
        buff.c("[");
        size_t width = ncols();
        for (int i = 0; i < width; i++) {
            Column* col = dynamic_cast<Column*>(columns_.get(i));
            const char* serial_col = col->serialize();
            buff.c(serial_col);
            delete[] serial_col;
        }
        buff.c("]");
        return buff.c_str();
    }

    /* Checks if this DataFrame equals the given object */
    bool equals(Object* other) {
        DataFrame* o = dynamic_cast<DataFrame*>(other);
        if (o == nullptr) { return false; }
        if (ncols() != o->ncols()) { return false; }
        if (nrows() != o->nrows()) { return false; }
        return columns_.equals(o->get_columns());
    }

    /**
     * Builds a DataFrame from rows created by the given visitor, adds the DataFrame to the
     * given KDStore at the given Key, and then returns the DataFrame.
     */
    static DataFrame* fromVisitor(Key* k, KDStore* kd, const char* scm, Writer& w);

    /**
     * Builds a DataFrame from an input file, adds the DataFrame to the given KDStore at the 
     * given Key, and then returns the DataFrame.
     */
    static DataFrame* fromFile(const char* filename, Key* k, KDStore* kd, char* len);

    /**
     * Builds a DataFrame with one column containing the data in the given int array, adds the
     * DataFrame to the given KDStore at the given Key, and then returns the DataFrame.
     */
    static DataFrame* fromIntArray(Key* k, KDStore* kd, size_t size, int* vals);

    /**
     * Builds a DataFrame with one column containing the data in the given bool array, adds the
     * DataFrame to the given KDStore at the given Key, and then returns the DataFrame.
     */
    static DataFrame* fromBoolArray(Key* k, KDStore* kd, size_t size, bool* vals);

    /**
     * Builds a DataFrame with one column containing the data in the given float array, adds the
     * DataFrame to the given KDStore at the given Key, and then returns the DataFrame.
     */
    static DataFrame* fromFloatArray(Key* k, KDStore* kd, size_t size, float* vals);

    /**
     * Builds a DataFrame with one column containing the data in the given string array, adds
     * the DataFrame to the given KDStore at the given Key, and then returns the DataFrame.
     */
    static DataFrame* fromStringArray(Key* k, KDStore* kd, size_t size, String** vals);

    /**
     * Builds a DataFrame with one column containing the single given int, adds the DataFrame
     * to the given KDStore at the given Key, and then returns the DataFrame.
     */
    static DataFrame* fromIntScalar(Key* k, KDStore* kd, int val);

    /**
     * Builds a DataFrame with one column containing the single given bool, adds the DataFrame
     * to the given KDStore at the given Key, and then returns the DataFrame.
     */
    static DataFrame* fromBoolScalar(Key* k, KDStore* kd, bool val);

    /**
     * Builds a DataFrame with one column containing the single given float, adds the DataFrame
     * to the given KDStore at the given Key, and then returns the DataFrame.
     */
    static DataFrame* fromFloatScalar(Key* k, KDStore* kd, float val);

    /**
     * Builds a DataFrame with one column containing the single given string, adds the DataFrame
     * to the given KDStore at the given Key, and then returns the DataFrame.
     */
    static DataFrame* fromStringScalar(Key* k, KDStore* kd, String* val);
};

// Deserializer functions defined below to avoid circular dependencies

/** Builds and returns a Chunk from the bytestream. */
Chunk* Deserializer::deserialize_chunk() {
    size_t idx = deserialize_size_t();
    size_t size = deserialize_size_t();
    Chunk* c = new Chunk(idx);
    assert(step() == '[');
    for (int i = 0; i < size; i++) {
        DataType* dt = deserialize_datatype();
        c->append(dt);
    }
    return c;
}

/** Builds and returns a DistributedVector from the bytestream. */
DistributedVector* Deserializer::deserialize_dist_vector(KVStore* kv) {
    StrBuff buff;
    size_t size = deserialize_size_t();
    assert(step() == '[');
    Vector* keys = new Vector();
    while (current() != ']')
        keys->append(deserialize_key());
    assert(step() == ']');
    return new DistributedVector(kv, size, keys);
}

/** Builds and returns a Column from the bytestream. */
Column* Deserializer::deserialize_column(KVStore* kv) {
    char type = step();
    DistributedVector* fields = deserialize_dist_vector(kv);
    return new Column(type, fields);
}

/** Builds and returns a DataFrame from the bytestream. */
DataFrame* Deserializer::deserialize_dataframe(KVStore* kv, Key* k) {
    assert(step() == '[');
    DataFrame* res = new DataFrame(kv, k);
    while (current() != ']') {
        Column* c = deserialize_column(kv);
        res->add_column(c);
    }
    assert(step() == ']');
    return res;
}