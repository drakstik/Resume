#pragma once

#include "vector.h"

/** Item_ are entries in a Map, they are not exposed, and are immutable.
 *  author: jv */
class Items_ {
public:
  Vector keys_; 
  Vector vals_; 

  Items_() : keys_(), vals_() {}
  
  Items_(Object *k, Object * v) : keys_(), vals_() {
    keys_.append(k);
    vals_.append(v);
  }

  bool contains_(Object& k) {
    for (int i = 0; i < keys_.size(); i++) 
      if (k.equals(keys_.get(i)))
	      return true;
    return false;
  }

  Object* get(Object& k) {
    for (int i = 0; i < keys_.size(); i++) 
      if (k.equals(keys_.get(i)))
	      return vals_.get(i);
    return nullptr;
  }

  size_t put(Object& k, Object* v) {
    for (int i = 0; i < keys_.size(); i++) 
      if (k.equals(keys_.get(i))) {
        vals_.set(v, i);
        return 0;
      }
    // The keys are owned, but the key is received as a reference, i.e. not owned so we must make a copy of it. 
    keys_.append(k.clone());
    vals_.append(v);
    return 1;
  }

  size_t remove(Object& k) {
    for (int i = 0; i < keys_.size(); i++) 
      if (k.equals(keys_.get(i))) {
        keys_.remove(i);
        vals_.remove(i);
        return 1;
      }
    return 0;
  }
};

/** A generic map class from Object to Object. Subclasses are responsibly of
 * making the types more specific.  author: jv */
class Map : public Object {
public:      
  size_t capacity_;
    // TODO this was not size of the map, but number of occupied item positions in the top level
  size_t size_ = 0;
  Items_* items_;  // owned

  Map() : Map(10) {}

  Map(size_t cap) {
    capacity_ = cap;
    items_ = new Items_[capacity_];
  }
  
  ~Map() { delete[] items_; }

  /** True if the key is in the map. */
  bool contains(Object& key)  { return items_[off_(key)].contains_(key); }
  
  /** Return the number of elements in the map. */
  size_t size()  {
      return size_;
  }

  size_t off_(Object& k) { return k.hash() % capacity_; }
  
  /** Get the value.  nullptr is allowed as a value.  */
  Object* get(Object &key) { return items_[off_(key)].get(key); }

  /** Add item->val_ at item->key_ either by updating an existing Item_ or
   * creating a new one if not found.  */
  void put(Object &k, Object *v) {
    if (size_ >= capacity_) grow();
    size_ += items_[off_(k)].put(k,v);
  }
  
  /** Removes element with given key from the map.  Does nothing if the
      key is not present.  */
  void erase(Object& k) { size_ -= items_[off_(k)].remove(k); }
  
  /** Resize the map, keeping all Item_s. */
  void grow() {
    Map newm(capacity_ * 2);
    for (size_t i = 0; i < capacity_; i++) {
        size_t sz = items_[i].keys_.size();
        for (size_t j = 0; j < sz; j++) {
            Object* k = items_[i].keys_.get(j);
            Object* v = items_[i].vals_.get(j);
            newm.put(*k,v);
            // otherwise the values would get deleted (if the array's destructor was doing its job I found later:)
            items_[i].vals_.set(nullptr, j, false);
        }
    }
    delete[] items_;
    items_ = newm.items_;
    capacity_ = newm.capacity_;
    assert(size_ == newm.size_);
    newm.items_ = nullptr;
  } 
}; // Map

class MutableString : public String {
public:
  MutableString() : String("", 0) {}
  void become(const char* v) {
    size_ = strlen(v);
    cstr_ = (char*) v;
    hash_ = hash_me();
  }
};


/***************************************************************************
 * 
 **********************************************************author:jvitek */
class Num : public Object {
public:
  size_t v = 0;
  Num() {}
  Num(size_t v) : v(v) {}
  Num* clone() { return new Num(v); }
};

class SIMap : public Map {
public:
  SIMap () {}
  Num* get(String& key) {
    Num* res = dynamic_cast<Num*>(Map::get(key));
    assert(res != nullptr);
    return res;
  }
  void put(String& k, Num* v) { 
    assert(v);
    Map::put(k, v);
  }
}; // SIMap