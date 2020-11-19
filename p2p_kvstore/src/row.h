//lang::CwC

#pragma once

#include "datatype.h"
#include "vector.h"
#include "schema.h"
#include "visitors.h"

/*************************************************************************
 * Row::
 *
 * This class represents a single row of data constructed according to a
 * dataframe's schema. The purpose of this class is to make it easier to add
 * read/write complete rows. Internally a dataframe hold data in columns.
 * Rows have pointer equality.
 * 
 * @author Spencer LaChance <lachance.s@northeastern.edu>
 * @author David Mberingabo <mberingabo.d@husky.neu.edu>
 */
class Row : public Object {
public:
    IntVector* col_types_;
    Vector* fields_;
    size_t idx_;

    /** Build a row following a schema. */
    Row(Schema& scm) {
        col_types_ = new IntVector();
        col_types_->append_all(scm.get_types());
        fields_ = new Vector();
        idx_ = -1;
    }

    /** Destructor */
    ~Row() {
        delete col_types_;
        delete fields_;
    }
    
    /** Setters: set the given column with the given value. Setting a column with
        * a value of the wrong type is undefined. */
    void set(size_t col, int val) {
        exit_if_not(col < width(), "Column index out of bounds.");
        exit_if_not(col_types_->get(col) == 'I', "Column index corresponds to the wrong type.");
        DataType* dt = new DataType();
        dt->set_int(val);
        fields_->set(dt, col);
    }
    void set(size_t col, float val) {
        exit_if_not(col < width(), "Column index out of bounds.");
        exit_if_not(col_types_->get(col) == 'F', "Column index corresponds to the wrong type.");
        DataType* dt = new DataType();
        dt->set_float(val);
        fields_->set(dt, col);
    }
    void set(size_t col, bool val) {
        exit_if_not(col < width(), "Column index out of bounds.");
        exit_if_not(col_types_->get(col) == 'B', "Column index corresponds to the wrong type.");
        DataType* dt = new DataType();
        dt->set_bool(val);
        fields_->set(dt, col);
    }
    /** Acquire ownership of the string. */
    void set(size_t col, String* val) {
        exit_if_not(col < width(), "Column index out of bounds.");
        exit_if_not(col_types_->get(col) == 'S', "Column index corresponds to the wrong type.");
        DataType* dt = new DataType();
        dt->set_string(val);
        fields_->set(dt, col);
    }
    
    /** Set/get the index of this row (ie. its position in the dataframe. This is
     *  only used for informational purposes, unused otherwise */
    void set_idx(size_t idx) {
        idx_ = idx;
    }
    size_t get_idx() {
        return idx_;
    }
    
    /** Getters: get the value at the given column. If the column is not
        * of the requested type, the result is undefined. */
    int get_int(size_t col) {
        exit_if_not(col < width(), "Column index out of bounds.");
        exit_if_not(col_types_->get(col) == 'I', "Column index corresponds to the wrong type.");
        DataType* type = dynamic_cast<DataType*>(fields_->get(col));
        return type->get_int();
    }
    bool get_bool(size_t col) {
        exit_if_not(col < width(), "Column index out of bounds.");
        exit_if_not(col_types_->get(col) == 'B', "Column index corresponds to the wrong type.");
        DataType* type = dynamic_cast<DataType*>(fields_->get(col));
        return type->get_bool();
    }
    float get_float(size_t col) {
        exit_if_not(col < width(), "Column index out of bounds.");
        exit_if_not(col_types_->get(col) == 'F', "Column index corresponds to the wrong type.");
        DataType* type = dynamic_cast<DataType*>(fields_->get(col));
        return type->get_float();
    }
    String* get_string(size_t col) {
        exit_if_not(col < width(), "Column index out of bounds.");
        exit_if_not(col_types_->get(col) == 'S', "Column index corresponds to the wrong type.");
        DataType* type = dynamic_cast<DataType*>(fields_->get(col));
        return type->get_string();
    }
    
    /** Number of fields in the row. */
    size_t width() {
        return col_types_->size();
    }
    
    /** Given a Fielder, visit every field of this row. The first argument is
        * index of the row in the dataframe.
        * Calling this method before the row's fields have been set is undefined. */
    void visit(size_t idx, Fielder& f) {
        f.start(idx);
        for (int i = 0; i < col_types_->size(); i++) {
            char type = col_types_->get(i);
            switch (type) {
                case 'I':
                    f.accept(get_int(i));
                    break;
                case 'B':
                    f.accept(get_bool(i));
                    break;
                case 'F':
                    f.accept(get_float(i));
                    break;
                case 'S':
                    f.accept(get_string(i));
                    break;
                default:
                    exit_if_not(false, "Invalid type found.");
            }
        }
        f.done();
    }

    /** Getter for this row's schema's types. */
    IntVector* get_types() { return col_types_; }

    /** Getter for this row's fields. */
    Vector* get_fields() { return fields_; }

    /* Returns true if the given Objcet is equal to this Row, otherwise returns false.
    *  Only used for Vectors containing Strings.
    *  */
    bool equals(Object* o) {
        Row* other = dynamic_cast<Row*>(o);
        if (other == nullptr) return false;
        if (other->width() != width()) return false;
        return other->get_fields()->equals(fields_) && other->get_types()->equals(col_types_) &&
            other->get_idx() == idx_;
    }
};