#include "../src/kdstore.h"

/******************************************************************************
 * Rower and Fielder implementations used in the test.
 *****************************************************************************/

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

/******************************************************************************
 * The test.
 *****************************************************************************/

int main(int argc, char** argv) {
    size_t idx = atoi(argv[2]);
    KDStore kd(idx, 3);
    Key ki("ints", 0);

    if (idx == 0) {
        // Build an array of CHUNK_SIZE 1's, CHUNK_SIZE 2's, and CHUNK_SIZE 3's
        int ints[CHUNK_SIZE * 3];
        for (int i = 0; i < CHUNK_SIZE * 3; i++) {
            // Node 0 will have all 1's stored in its k/v store
            if (i < CHUNK_SIZE)
                ints[i] = 1;
            // Node 1 will have all 2's stored in its k/v store
            else if (i < CHUNK_SIZE * 2)
                ints[i] = 2;
            // Node 2 will have all 3's stored in its k/v store
            else if (i < CHUNK_SIZE * 3)
                ints[i] = 3;
        }
        // Add the array to a DataFrame and store it in the k/v store
        delete DataFrame::fromIntArray(&ki, &kd, CHUNK_SIZE * 3, ints);
    }

    // Run local_map() on the dataframe and verify that the sums for all 3 nodes are correct
    DataFrame* ints = kd.wait_and_get(ki);
    SumRower sr;
    ints->local_map(sr);

    switch (idx) {
        case 0: assert(sr.get_total() == CHUNK_SIZE);     break;
        case 1: assert(sr.get_total() == CHUNK_SIZE * 2); break;
        case 2: assert(sr.get_total() == CHUNK_SIZE * 3); break;
    }

    Sys s;
    s.p("Node ", idx).p(idx, idx).pln(": Local map test passed.", idx);

    delete ints;
    if (idx == 0) {
        sleep(2);
        kd.done();
    }
    return 0;
}