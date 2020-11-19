//lang::CwC

#pragma once

#include "serial.h"

/**
 * A union describing a field that could exist in a row of a dataframe.
 * 
 * @author Spencer LaChance <lachance.s@northeastern.edu>
 * @author David Mberingabo <mberingabo.d@husky.neu.edu>
 */
union Type {
    int i;
    bool b;
    float f;
    String* s;
};

/**
 * An Object wrapper class for the Type union. It is only allowed to one value of one of the four
 * types.
 * 
 * @author Spencer LaChance <lachance.s@northeastern.edu>
 * @author David Mberingabo <mberingabo.d@husky.neu.edu>
 */
class DataType : public Object {
public:
    union Type t_;
    char type_;

    /**
     * Constructor
     */
    DataType() : type_('U') { }

    /**
     * Destructor
     */
    ~DataType() {
        if (type_ == 'S') delete t_.s; 
    }

    /**
     * These setters set the value of this object to the given one. Once a value is set, the
     * object's type is locked down and cannot change.
     */
    void set_int(int val) {
        exit_if_not(type_ == 'U', "This object's value has already been set to a different type.");
        t_.i = val;
        type_ = 'I';
    }
    void set_bool(bool val) {
        exit_if_not(type_ == 'U', "This object's value has already been set to a different type.");
        t_.b = val;
        type_ = 'B';
    }
    void set_float(float val) {
        exit_if_not(type_ == 'U', "This object's value has already been set to a different type.");
        t_.f = val;
        type_ = 'F';
    }
    // Takes ownership of the string.
    void set_string(String* val) {
        exit_if_not(type_ == 'U', "This object's value has already been set to a different type.");
        t_.s = val;
        type_ = 'S';
    }

    /**
     * These getters return this object's value.
     */
    int get_int() {
        if (type_ == 'U') set_int(0); // Return default value if missing
        exit_if_not(type_ == 'I', "This object's type does not match the type requested.");
        return t_.i;
    }
    bool get_bool() {
        if (type_ == 'U') set_bool(0); // Return default value if missing
        exit_if_not(type_ == 'B', "This object's type does not match the type requested.");
        return t_.b;
    }
    float get_float() {
        if (type_ == 'U') set_float(0); // Return default value if missing
        exit_if_not(type_ == 'F', "This object's type does not match the type requested.");
        return t_.f;
    }
    String* get_string() {
        if (type_ == 'U') set_string(new String("")); // Return default value if missing
        exit_if_not(type_ == 'S', "This object's type does not match the type requested.");
        return t_.s;
    }

    /**
     * Returns this DataType's type char.
     * */
    char get_type() { return type_; }

    /** Returns a copy of this DataType. */
    DataType* clone() {
        DataType* res = new DataType();
        switch (type_) {
            case 'I':
                res->set_int(t_.i); break;
            case 'B':
                res->set_bool(t_.b); break;
            case 'F':
                res->set_float(t_.f); break;
            case 'S':
                res->set_string(t_.s->clone()); break;
        }
        return res;
    }

    /** Returns a char* representation of this DataType. */
    const char* serialize() {
        StrBuff buff;
        // Serialize the type char
        char* c_type = new char[2];
        c_type[0] = type_;
        c_type[1] = '\0';
        buff.c(c_type);
        delete[] c_type;
        // Serialize the value
        const char* serial_val;
        switch (type_) {
            case 'I': {
                serial_val = Serializer::serialize_int(t_.i);
                buff.c(serial_val);
                delete[] serial_val;
                break;
            }
            case 'B': {
                serial_val = Serializer::serialize_bool(t_.b);
                buff.c(serial_val);
                delete[] serial_val;
                break;
            }
            case 'F': {
                serial_val = Serializer::serialize_float(t_.f);
                buff.c(serial_val);
                delete[] serial_val;
                break;
            }
            case 'S': {
                serial_val = t_.s->serialize();
                buff.c(serial_val);
                delete[] serial_val;
                break;
            }
        }
        return buff.c_str();
    }

    bool equals(Object* o) {
        DataType* other = dynamic_cast<DataType*>(o);
        if (other == nullptr) return false;
        if (type_ != other->get_type()) return false;
        switch (type_) {
            case 'I':
                return other->get_int() == get_int();
            case 'B':
                return other->get_bool() == get_bool();
            case 'F':
                return other->get_float() == get_float();
            case 'S':
                return other->get_string()->equals(get_string());
            case 'U':
                return true;
        }
    }
};
