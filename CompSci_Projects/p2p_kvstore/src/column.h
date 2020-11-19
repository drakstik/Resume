//lang:CwC

#pragma once

#include <stdarg.h>
#include "dist_vector.h"

/*************************************************************************
 * DataFrame Column
 * Holds values of a certain type.
 * 
 * @author Spencer LaChance <lachance.s@northeastern.edu>
 * @author David Mberingabo <mberingabo.d@husky.neu.edu>
 */
class Column : public Object {
public:
    // DistributedVector containing all of this Column's fields
    DistributedVector* fields_;
    char type_;
    
    /** Constructs an empty Column */
    Column(char type, KVStore* kv, Key* k) : type_(type), fields_(new DistributedVector(kv, k)) {
        exit_if_not(type == 'I' || type == 'B' || type == 'F' || type == 'S',
            "Invalid Column type");
    }

    /** Constructs a Column containing the fields in the given DVector. */
    Column(char type, DistributedVector* fields) : type_(type), fields_(fields) {
        exit_if_not(type == 'I' || type == 'B' || type == 'F' || type == 'S',
            "Invalid Column type");
    }

    /** Constructs a Column initialized with the fields in the given va_list. */
    Column(char type, KVStore* kv, Key* k, int n, ...) : 
        type_(type), fields_(new DistributedVector(kv, k)) {
        va_list vl;
        va_start(vl, n);
        for (int i = 0; i < n; i++) {
            DataType* dt = new DataType();
            switch(type_) {
                case 'I':
                    dt->set_int(va_arg(vl, int)); break;
                case 'B':
                    dt->set_bool(va_arg(vl, int)); break;
                case 'F':
                    dt->set_float(va_arg(vl, double)); break;
                case 'S':
                    dt->set_string(va_arg(vl, String*)); break;
            }
            fields_->append(dt);
        }
        va_end(vl);
        lock();
    }

    /** Destructor */
    ~Column() { delete fields_; }

    /** Adds the given int to the end of the column. */
    void push_back(int val) {
        exit_if_not(type_ = 'I', "Column type is not integer");
        DataType* dt = new DataType();
        dt->set_int(val);
        fields_->append(dt);
    }

    /** Adds the given bool to the end of the column. */
    void push_back(bool val) {
        exit_if_not(type_ = 'B', "Column type is not boolean");
        DataType* dt = new DataType();
        dt->set_bool(val);
        fields_->append(dt);
    }

    /** Adds the given float to the end of the column. */
    void push_back(float val) {
        exit_if_not(type_ = 'F', "Column type is not float");
        DataType* dt = new DataType();
        dt->set_float(val);
        fields_->append(dt);
    }

    /** Adds the given string to the end of the column. */
    void push_back(String* val) {
        exit_if_not(type_ = 'S', "Column type is not string");
        DataType* dt = new DataType();
        dt->set_string(val);
        fields_->append(dt);
    }

    /** Gets the int at the specified index. */
    int get_int(size_t idx) {
        exit_if_not(type_ = 'I', "Column type is not integer");
        DataType* dt = fields_->get(idx);
        int res = dt->get_int();
        delete dt;
        return res;
    }

    /** Gets the bool at the specified index. */
    bool get_bool(size_t idx) {
        exit_if_not(type_ = 'B', "Column type is not boolean");
        DataType* dt = fields_->get(idx);
        bool res = dt->get_bool();
        delete dt;
        return res;
    }

    /** Gets the float at the specified index. */
    float get_float(size_t idx) {
        exit_if_not(type_ = 'F', "Column type is not float");
        DataType* dt = fields_->get(idx);
        float res = dt->get_float();
        delete dt;
        return res;
    }

    /** Gets the string at the specified index. */
    String* get_string(size_t idx) {
        exit_if_not(type_ = 'S', "Column type is not string");
        DataType* dt = fields_->get(idx);
        String* res = dt->get_string()->clone();
        delete dt;
        return res;
    }

    /** Returns the index of the node on which the field at idx is stored. */
    size_t get_node(size_t idx) { return fields_->get_node(idx); }

    /** Returns the number of fields in this Column. */
    size_t size() { return fields_->size(); }

    /** Getter for this column's underlying array of fields. */
    DistributedVector* get_fields() { return fields_; }

    /** Getter for this column's type. */
    char get_type() { return type_; }

    /** Appends an empty DataType that represents a missing field */
    void append_missing() {
        DataType* dt = new DataType();
        fields_->append(dt);
    }

    /** Called when all fields have been added to this column. */
    void lock() { fields_->lock(); }

    /** Called when more fields must be added to this locked column. */
    void unlock() { fields_->unlock(); }

    /** Returns a serialized representation of this Column. */
    const char* serialize() {
        StrBuff buff;
        // Serialize the type char
        char* type = new char[2];
        type[0] = type_;
        type[1] = '\0';
        buff.c(type);
        delete[] type;
        // Serialize the DistributedVector
        const char* serial_vec = fields_->serialize();
        buff.c(serial_vec);
        delete[] serial_vec;
        return buff.c_str();
    }

    /* Is this column equal to the given object? */
    bool equals(Object* other) {
        Column* o = dynamic_cast<Column*>(other);
        if (o == nullptr) return false;
        return type_ == o->get_type() && o->get_fields()->equals(get_fields());
    }
};
