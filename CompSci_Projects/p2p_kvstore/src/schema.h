//lang::CwC

#pragma once

#include "object.h"
#include "vector.h"

/*************************************************************************
 * Schema::
 * A schema is a description of the contents of a data frame, the schema
 * knows the number of columns and the type of each column.
 * The valid types are represented by the chars 'S', 'B', 'I' and 'F'.
 * 
 * @author Spencer LaChance <lachance.s@northeastern.edu>
 * @author David Mberingabo <mberingabo.d@husky.neu.edu>
 */
class Schema : public Object {
public:
    IntVector* col_types_;

    /** Copying constructor */
    Schema(Schema& from) {
        col_types_ = new IntVector();
        col_types_->append_all(from.get_types());
    }
    
    /** Create an empty schema **/
    Schema() {
        col_types_ = new IntVector();
    }
    
    /** Create a schema from a string of types. A string that contains
     *  characters other than those identifying the four type results in
     *  undefined behavior. The argument is external, a nullptr argument is
     *  undefined. **/
    Schema(const char* types) {
        exit_if_not(types != nullptr, "Null types argument provided.");
        col_types_ = new IntVector();
        for (int i = 0; i < strlen(types); i++) {
            char c = types[i];
            exit_if_not(c == 'S' || c == 'B' || c == 'I' || c == 'F', 
                "Invalid type character found.");
            col_types_->append(c);
        }
    }

    /** Destructor */
    ~Schema() {
        delete col_types_;
    }
    
    /** Add a column of the given type and name (can be nullptr), name
     *  is external. Names are expectd to be unique, duplicates result
     *  in undefined behavior. */
    void add_column(char typ) {
        col_types_->append(typ);
    }
    
    /** Return type of column at idx. An idx >= width is undefined. */
    char col_type(size_t idx) {
        exit_if_not(idx < width(), "Column type index out of bounds.");
        return col_types_->get(idx);
    }
    
    /** The number of columns */
    size_t width() {
        return col_types_->size();
    }

    /** Getter for the array of column types */
    IntVector* get_types() {
        return col_types_;
    }

    /** Resolves is this schema equal to the given object? **/
    bool equals(Object* o) {
        Schema* other = dynamic_cast<Schema*>(o);
        if (other == nullptr) return false;
        return get_types()->equals(other->get_types());
    }
};