//lang::CwC

#include "../src/application.h"
#include "../src/dataframe.h"

// Constants that allow us to use a different number of fields when running valgrind
bool valgrind = false;
size_t size = 1000000;
size_t valgrind_size = 10000;

/**
 * Simple eau2 application that builds a DataFrame containing floats 0-999,999 keeps track of the
 * floats' sum, puts the DataFrame into a KVStore, gets it back from the store, and verifies that
 * the sum of the DataFrame's values remains consistent.
 */
class Trivial : public Application {
public:
    Trivial(size_t idx, size_t nodes) : Application(idx, nodes) {
        run_();
    }
    void run_() {
        size_t SZ = valgrind ? valgrind_size : size;
        float* vals = new float[SZ];
        double sum = 0;
        for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
        Key* key = new Key("triv",0);
        DataFrame* df = DataFrame::fromFloatArray(key, &kd_, SZ, vals);
        assert(df->get_float(0, 1) == 1);
        DataFrame* df2 = kd_.get(*key);
        for (size_t i = 0; i < SZ; ++i) sum -= df2->get_float(0,i);
        assert(sum == 0);
        printf("KVStore Trivial test passed\n");
        done();
        delete df; delete df2; delete[] vals; delete key;
    }
};

int main(int argc, char** argv) {
    // Check if app is being run with valgrind
    if (argc > 1 && strcmp(argv[1], "-v") == 0) valgrind = true;
    Trivial t(0, 1);
    return 0;
}