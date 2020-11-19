#include "../src/application.h"
#include "../src/dataframe.h"

// Constants that allow us to use a different number of fields when running valgrind
bool valgrind = false;
size_t size = 100000;
size_t valgrind_size = 2000;

class Demo : public Application {
public:
    Key* main;
    Key* verify;
    Key* check;

    Demo(size_t idx, size_t num_nodes) : 
        Application(idx, num_nodes), main(new Key("main", 0)), verify(new Key("verif", 0)), 
        check(new Key("ck", 0)) {
        run_();
    }

    ~Demo() {
        delete main;
        delete verify;
        delete check;
    }

    void run_() {
        switch(this_node()) {
            case 0:   producer();   break;
            case 1:   counter();    break;
            case 2:   summarizer();
        }
    }

    void producer() {
        size_t SZ = valgrind ? valgrind_size : size;
        float* vals = new float[SZ];
        float sum = 0;
        for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
        delete DataFrame::fromFloatArray(main, &kd_, SZ, vals);
        delete DataFrame::fromFloatScalar(check, &kd_, sum);
        delete[] vals;
    }

    void counter() {
        size_t SZ = valgrind ? valgrind_size : size;
        DataFrame* v = kd_.wait_and_get(*main);
        float sum = 0;
        for (size_t i = 0; i < SZ; ++i) sum += v->get_float(0,i);
        p("The sum is ", this_node()).pln(sum, this_node());
        delete DataFrame::fromFloatScalar(verify, &kd_, sum);
        delete v;
    }

    void summarizer() {
        DataFrame* result = kd_.wait_and_get(*verify);
        DataFrame* expected = kd_.wait_and_get(*check);
        pln(expected->get_float(0,0)==result->get_float(0,0) ? "SUCCESS":"FAILURE", this_node());
        done();
        delete result; delete expected;
    }
};

int main(int argc, char** argv) {
    Sys s;
    size_t idx;
    if (strcmp(argv[1], "-v") == 0) {
        // Demo is being run with valgrind
        valgrind = true;
        idx = atoi(argv[3]);
    } else {
        idx = atoi(argv[2]);
    }
    s.exit_if_not(idx <= 2, "Invalid index from command line");
    Demo d(idx, 3);
}