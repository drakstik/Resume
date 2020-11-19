//lang::CwC

#include <unistd.h>
#include "../src/dataframe.h"
#include "../src/parser_main.h"
#include "../src/helper.h"

// The number of rows in the DataFrame we build below.
#define NROWS 10000

/*................................................................................................*/
/*...........................IMPLEMENTATION OF ROWERS AND FIELDERS................................*/
/**
 * A Fielder that adds up every int it finds in a row.
 * 
 * @author David Mberingabo <mberingabo.d@husky.neu.edu>
 * @author Spencer LaChance <lachance.s@northeastern.edu>
 */
class SumFielder : public Fielder {
public:
    long total_;

    void start(size_t r) { total_ = 0; }
    void done() { }

    SumFielder(long total) {
        total_ = total;
    }
    ~SumFielder() { }

    void accept(bool b) { }
    void accept(float f) { }
    void accept(String* s) { }
    void accept(int i) {
        total_ += i;
    }

    long get_total() {
        return total_;
    }
};

/**
 * A Rower that adds up every int it finds in a row using a Fielder.
 * 
 * @author David Mberingabo <mberingabo.d@husky.neu.edu>
 * @author Spencer LaChance <lachance.s@northeastern.edu>
 */
class SumRower : public Rower {
public:
    SumFielder* sf_;
    long total_;

    SumRower() {
        total_ = 0;
        sf_ = new SumFielder(total_);
    }
    ~SumRower() { delete sf_; }

    bool accept(Row& r) {
        r.visit(r.get_idx(), *sf_);
        total_ += sf_->get_total();
    }

    long get_total() {
        return total_;
    }

    void join_delete(Rower* other) {
        SumRower* o = dynamic_cast<SumRower*>(other);
        total_ += o->get_total();
        delete o;
    }

    Object* clone() {
        return new SumRower();
    }
};

/**
 * A Fielder that accepts every int in a row that is above a given threshhold.
 * 
 * @author David Mberingabo <mberingabo.d@husky.neu.edu>
 * @author Spencer LaChance <lachance.s@northeastern.edu>
 */
class AboveFielder : public Fielder {
public:
    bool passes_;
    int thresh_;

    void start(size_t r) {passes_ = true;}
    void done() { }

    AboveFielder(int thresh) {
        thresh_ = thresh;
    }
    ~AboveFielder() { }

    void accept(bool b) { }
    void accept(float f) { }
    void accept(String* s) { }
    void accept(int i) {
        if (i <= thresh_) passes_ = false;
    }

    bool check_pass() {
        return passes_;
    }
};

/**
 * A Rower that accepts every row whose ints are above a given threshhold.
 * 
 * @author David Mberingabo <mberingabo.d@husky.neu.edu>
 * @author Spencer LaChance <lachance.s@northeastern.edu>
 */
class AboveRower : public Rower {
public:
    AboveFielder* af_;

    AboveRower(int thresh) {
        af_ = new AboveFielder(thresh);
    }

    ~AboveRower() { delete af_; }

    bool accept(Row& r) {
        r.visit(r.get_idx(), *af_);
        return af_->check_pass();
    }

    void join_delete(Rower* other) { }
};

/**
 * A simple test that tests map() using a Rower that calculates the sum of every int in the
 * dataframe.
 */
void test_map(DataFrame* df) {
    SumRower* sr = new SumRower();
    df->map(*sr);
    assert(sr->get_total() == (NROWS + 1)*(NROWS / 2));
    printf("DataFrame map() test passed\n");
    delete sr;
}

/**
 * A simple test that tests filter() using a Rower that accepts all rows with ints greater
 * than the given value.
 */
void test_filter(DataFrame* df) {
    AboveRower* ar = new AboveRower(NROWS / 2);
    DataFrame* filtered_df = df->filter(*ar);
    assert(filtered_df->nrows() == NROWS / 2);
    printf("DataFrame filter() test passed\n");
    delete ar;
    delete filtered_df;
}

/**
 * This test builds a DataFrame from data in a file and then calculates the sum of all of its ints.
 * This test is meant to measure performance and correct data parsing, 
 * so it does not verify that the sum is correct.
 */
void test_datafile(int argc, const char** argv, KVStore* kv) {
    // Upon construction, this class reads the command line arguments and builds a dataframe
    // containing fields from a datafile.
    Key* k2 = new Key("bar", 0);
    ParserMain* pf = new ParserMain(argc, argv, kv, k2);

    DataFrame* df = pf->get_dataframe();
    SumRower* sr = new SumRower();
    df->map(*sr);
    delete pf;
    delete df;
    delete sr;
    delete k2;
    printf("Datafile test passed\n");
}

/** Unit tests for DataFrame, Row, and Column classes */
void test_rows_cols(DataFrame* df, KVStore* kv, Key* k) {
    // Build the row, using the df's schema.
    Row* r1 = new Row(df->get_schema());
    df->fill_row(NROWS - 1, *r1);
    Row* r2 = new Row(df->get_schema());
    r2->set(0, NROWS);
    String* str2 = new String("foo");
    r2->set(1, str2);
    assert(r1->equals(r2));

    KeyBuff kbuf(k);
    // Build a bool column with NROWS / 2 values alternating between true and false.
    kbuf.c("-c3");
    Column* b_col = new Column('B', kv, kbuf.get(0));
    for (int i = 1; i <= NROWS / 2; i++) {
        bool b = i%2;
        b_col->push_back(b);
    }
    b_col->lock();

    // Build a float column with NROWS values from 1.1 to NROWS.1, increasing by 1.
    kbuf.c("-c2");
    Column* f_col = new Column('F', kv, kbuf.get(0));
    for (int i = 1; i <= NROWS; i++) {
        f_col->push_back(i+0.1f);
    }
    f_col->lock();

    // Adding a float column to an empty DataFrame
    df->add_column(f_col);
    assert(df->ncols() == 3);
    assert(df->nrows() == NROWS);

    // Adding a bool column with NROWS / 2 values to a DataFrame with NROWS values.
    assert(b_col->size() == NROWS / 2);
    df->add_column(b_col);
    assert(b_col->size() == NROWS);

    // Retrieving a bool column from df
    Column* padded_b_col = dynamic_cast<Column*>(df->get_columns()->get(3));

    // Testing padding of columns added without the same length as the DataFrame.
    assert(padded_b_col != nullptr);
     // b_col was padded with missing values when it was added to df
    assert(padded_b_col->size() == NROWS);
    for (int i = NROWS / 2; i < NROWS; i++) {
        // missing value is represented as false in BoolColumns.
        assert(padded_b_col->get_bool(i) == false);
    }

    Row* r3 = new Row(df->get_schema());
    df->fill_row(0, *r3);

    assert(df->get_int(0, 0) == r3->get_int(0));
    String* df_str = df->get_string(1, 0);
    assert(df_str->equals(r3->get_string(1)));
    assert(df->get_float(2, 0) == r3->get_float(2));
    assert(df->get_bool(3, 0) == r3->get_bool(3));
    
    delete r1;
    delete r2;
    delete r3;
    delete df_str;
    printf("Rows and columns test passed\n");
}

int main(int argc, const char** argv) {
    Schema s("IS");
    KVStore* kv = new KVStore(0, 1);
    Key* k1 = new Key("foo", 0);
    DataFrame* df = new DataFrame(s, kv, k1);
    Row r(s);
    String* str = new String("foo");
    for (int i = 1; i <= NROWS; i++) {
        r.set(0, i);
        r.set(1, str->clone());
        if (i == NROWS) df->add_row(r, true);
        else            df->add_row(r, false);
    }

    test_map(df);
    test_filter(df);
    test_rows_cols(df, kv, k1);
    test_datafile(argc, argv, kv);

    kv->shutdown();
    delete kv;
    delete k1;
    delete df;
    delete str;
    return 0;
}
