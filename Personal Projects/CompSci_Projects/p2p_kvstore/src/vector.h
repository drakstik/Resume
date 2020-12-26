#pragma once

#include <stdbool.h>
#include <assert.h>
#include "string.h"
#include "serial.h"

// The number of chunks held initially in each vector
#define INITIAL_CHUNK_CAPACITY 1024
// The number of items that each chunk holds
#define CHUNK_SIZE 5000

/**
 * Represents an vector (Java: ArrayList) of objects.
 * In order to have constant time lookup and avoid copying the payload of the 
 * array, this data structure is an array of pointers to chunks of objects. It 
 * is essentially a 2D array. The chunks have a fixed length and when the array 
 * of chunks runs out of space, a new array is allocated with more memory and 
 * the chunk pointers are transferred to it.
 * 
 * @author Spencer LaChance <lachance.s@northeastern.edu>
 * @author David Mberingabo <mberingabo.d@husky.neu.edu>
 */
class Vector : public Object {
public:
    Object*** objects_;   
    int size_;
    // Number of chunks that we have space for
    int chunk_capacity_; 
    // Number of chunks that have been initialized
    int chunk_count_;

    /**
     * Initialize an empty Vector.
     */
    Vector() {
        size_ = 0;
        chunk_capacity_ = INITIAL_CHUNK_CAPACITY;
        chunk_count_ = 1;
        objects_ = new Object**[chunk_capacity_];
        objects_[0] = new Object*[CHUNK_SIZE];
        // Initialize all values in the new chunk to default value nullptr
        for (int i = 0; i < CHUNK_SIZE; i++) {
            objects_[0][i] = nullptr;
        }
        // Initialize all other chunk uninitialized chunks to default value nullptr
        for (int i = 1; i < chunk_capacity_; i++) {
            objects_[i] = nullptr;
        }
    }

    /**
     * Destructor for a Vector
     */
    ~Vector() {
        // Delete each chunk
        for (int i = 0; i < chunk_capacity_; i++) {
            if (objects_[i] != nullptr) {
                // Delete each object in the chunk
                for (int j = 0; j < CHUNK_SIZE; j++) {
                    if (objects_[i][j] != nullptr) delete objects_[i][j];
                }
                delete[] objects_[i];
            }
        }
        // Delete the array that holds the chunks
        delete[] objects_;
    }

    /**
     * Private function that reallocates more space for the vector
     * once it fills up.
     */
    void reallocate_() {
        Object*** new_outer_arr = new Object**[chunk_capacity_ * 2];

        Object*** old_outer_arr = objects_;
        for (int i = 0; i < chunk_count_; i++) {
            new_outer_arr[i] = old_outer_arr[i];
        }
        delete[] old_outer_arr;
        objects_ = new_outer_arr;

        chunk_capacity_ *= 2;
        // Initialize all other uninitialized chunks to default value nullptr
        for (int i = chunk_count_; i < chunk_capacity_; i++) {
            objects_[i] = nullptr;
        }
    }
    
    // Appends val to the end of the vector. Takes control of the val.
    void append(Object* val) {
        // If all of the chunks are full, allocate more memory for the outer array.
        if (size_ + 1 > chunk_capacity_ * CHUNK_SIZE) reallocate_();
        // If the last chunk is full, initialize a new one and add val to it.
        if (size_ + 1 > chunk_count_ * CHUNK_SIZE) {
            objects_[chunk_count_] = new Object*[CHUNK_SIZE];
            objects_[chunk_count_][0] = val;
            // Initialize all values in the new chunk to default value nullptr
            for (int i = 1; i < CHUNK_SIZE; i++) {
                objects_[chunk_count_][i] = nullptr;
            }
            chunk_count_++;
        } else {
            objects_[size_ / CHUNK_SIZE][size_ % CHUNK_SIZE] = val;
        }
        size_++;
    }
    
    // Appends a clone of every element of vals to the end of the vector.
    // If vals is null, does nothing.
    void append_all(Vector* vals) {
        if (vals == NULL) return;
        for (int i = 0; i < vals->size(); i++) {
            Object* val = vals->get(i);
            if (val == nullptr) {
                append(nullptr);
            } else {
                append(val->clone());
            }
        }
    }
    
    // Sets the element at index to val.
    // If index == size(), appends to the end of the vector.
    // If delete_val is true, the existing value at index will be deleted.
    void set(Object* val, size_t index, bool delete_val = true) {
        assert(index <= size_);

        if (index == size_) {
            append(val);
            return;
        }

        int outer_idx = index / CHUNK_SIZE;
        int inner_idx = index % CHUNK_SIZE;
        if (delete_val) {
            // Delete the object at this index if there is one
            Object* replace_me = dynamic_cast<Object*>(objects_[outer_idx][inner_idx]);
            if (replace_me != nullptr) delete replace_me;
        }
        objects_[outer_idx][inner_idx] = val;
    }
    
    // Gets the element at the given index.
    Object* get(size_t index) {
        assert(index < size_);
        int outer_idx = index / CHUNK_SIZE;
        int inner_idx = index % CHUNK_SIZE;
        return objects_[outer_idx][inner_idx];
    }

    // Removes the element at the given index.
    void remove(size_t index) {
        assert(index < size_);
        int outer_idx = index / CHUNK_SIZE;
        int inner_idx = index % CHUNK_SIZE;
        delete objects_[outer_idx][inner_idx];
        objects_[outer_idx][inner_idx] = nullptr;
        size_--;
    }
    
    // Returns the number of elements in this vector. 
    size_t size() {
        return size_;
    }

    // Inherited from Object
    // Is this Vector equal to the given Object?
    bool equals(Object* o) {
        Vector* other = dynamic_cast<Vector*>(o);
        if (other == nullptr) return false;
        if (size_ == 0) return other->size() == 0;
        for (int i = 0; i < size_; i++) {
            if (!get(i)->equals(other->get(i))) return false;
        }
        return true;
    }

    /**
     * Returns a serialized version of this Vector as a char*
     */
    const char* serialize() {
        StrBuff buff;
        // serialize the size
        char* serial_size = Serializer::serialize_size_t(size_);
        buff.c(serial_size);
        delete[] serial_size;
        // serialize the objects
        for (int i = 0; i < size_; i++) {
            const char* serial_obj = get(i)->serialize();
            buff.c(serial_obj);
            delete[] serial_obj;
        }
        return buff.c_str();
    }
};

/**
 * Represents an vector (Java: ArrayList) of integers.
 * In order to have constant time lookup and avoid copying the payload of the 
 * array, this data structure is an array of pointers to chunks of objects. It 
 * is essentially a 2D array. The chunks have a fixed length and when the array 
 * of chunks runs out of space, a new array is allocated with more memory and 
 * the chunk pointers are transferred to it.
 * 
 * @author Spencer LaChance <lachance.s@northeastern.edu>
 * @author David Mberingabo <mberingabo.d@husky.neu.edu>
 */
class IntVector : public Object {
public:
    int** ints_;
    int size_;
    // Number of chunks that we have space for
    int chunk_capacity_; 
    // Number of chunks that have been initialized
    int chunk_count_;

    /**
     * Constructor for an IntVector.
     * 
    */ 
    IntVector() {
        size_ = 0;
        chunk_capacity_ = INITIAL_CHUNK_CAPACITY;
        chunk_count_ = 1;
        ints_ = new int*[chunk_capacity_];
        ints_[0] = new int[CHUNK_SIZE];
        // Initialize all other uninitialized chunks to default value nullptr
        for (int i = 1; i < chunk_capacity_; i++) {
            ints_[i] = nullptr;
        }
    } 

    /**
     * Destructor for an IntVector.
     */ 
    ~IntVector() {
        // Delete each chunk
        for (int i = 0; i < chunk_capacity_; i++) {
            if (ints_[i] != nullptr) delete[] ints_[i];
        }
        // Delete the array that holds the chunks
        delete[] ints_;
    }

    /*
        * Private function that reallocates more space for the IntVector
        * once it fills up.
        */
    void reallocate_() {
        int** new_outer_arr = new int*[chunk_capacity_ * 2]; 

        int** old_outer_arr = ints_;
        for (int i = 0; i < chunk_count_; i++) {
            new_outer_arr[i] = old_outer_arr[i];
        }
        delete[] old_outer_arr;
        ints_ = new_outer_arr;

        chunk_capacity_ *= 2;
        // Initialize all other uninitialized chunks to default value nullptr
        for (int i = chunk_count_; i < chunk_capacity_; i++) {
            ints_[i] = nullptr;
        }
    }
    
    // Appends val onto the end of the vector
    void append(int val) {
        // If all of the chunks are full, allocate more memory for the outer array.
        if (size_ + 1 > chunk_capacity_ * CHUNK_SIZE) reallocate_();
        // If the last chunk is full, initialize a new one and add val to it.
        if (size_ + 1 > chunk_count_ * CHUNK_SIZE) {
            ints_[chunk_count_] = new int[CHUNK_SIZE];
            ints_[chunk_count_][0] = val;
            chunk_count_++;
        } else {
            ints_[chunk_count_ - 1][size_ % CHUNK_SIZE] = val;
        }
        size_++;
    }
    
    // Appends every element of vals to the end of the vector.
    // If vals is null, does nothing.
    void append_all(IntVector* vals) {
        if (vals == NULL) return;
        for (int i = 0; i < vals->size(); i++) {
            append(vals->get(i));
        }
    }
    
    // Sets the element at index to val.
    // If index == size(), appends to the end of the vector.
    void set(int val, size_t index) {
        assert(index <= size_);

        if (index == size_) {
            append(val);
            return;
        }

        int outer_idx = index / CHUNK_SIZE;
        int inner_idx = index % CHUNK_SIZE;
        ints_[outer_idx][inner_idx] = val;
    }
    
    // Gets the element at index.
    // If index is >= size(), does nothing and returns undefined.
    int get(size_t index) {
        assert(index < size_);
        int outer_idx = index / CHUNK_SIZE;
        int inner_idx = index % CHUNK_SIZE;
        return ints_[outer_idx][inner_idx];
    }
    
    // Returns the number of elements.
    size_t size() {
        return size_;
    }

    // Inherited from Object
    // Is this IntVector equal to the given Object?
    bool equals(Object* o) {
        IntVector* other = dynamic_cast<IntVector*>(o);
        if (other == nullptr) return false;
        if (size_ == 0) return other->size() == 0;

        for (int i = 0; i < size_; i++) {
            if (!(get(i) == other->get(i))) return false;
        }

        return true;
    }

    /** 
     * Returns a serialized representation of this int vector.
     */
    const char* serialize() {
        StrBuff buff;
        // serialize the size
        char* serial_size = Serializer::serialize_size_t(size_);
        buff.c(serial_size);
        delete[] serial_size;
        // serialize the ints
        for (int i = 0; i < size_; i++) {
            char* serial_int = Serializer::serialize_int(get(i));
            buff.c(serial_int);
            delete[] serial_int;
        }
        return buff.c_str();
    }
};