//lang::CwC

#pragma once

#include "object.h"
#include "string.h"

class Row;

/*****************************************************************************
 * Fielder::
 * A field vistor invoked by Row.
 */
class Fielder : public Object {
public:
 
  /** Called before visiting a row, the argument is the row offset in the
    dataframe. */
  virtual void start(size_t r) = 0;
 
  /** Called for fields of the argument's type with the value of the field. */
  virtual void accept(bool b) = 0;
  virtual void accept(float f) = 0;
  virtual void accept(int i) = 0;
  virtual void accept(String* s) = 0;
 
  /** Called when all fields have been seen. */
  virtual void done() = 0;
};
 
/*******************************************************************************
 *  Rower::
 *  An interface for iterating through each row of a data frame. The intent
 *  is that this class should subclassed and the accept() method be given
 *  a meaningful implementation. Rowers can be cloned for parallel execution.
 */
class Rower : public Object {
 public:
  /** This method is called once per row. The row object is on loan and
      should not be retained as it is likely going to be reused in the next
      call. The return value is used in filters to indicate that a row
      should be kept. */
  virtual bool accept(Row& r) = 0;
 
  /** Once traversal of the data frame is complete the rowers that were
      split off will be joined.  There will be one join per split. The
      original object will be the last to be called join on. The join method
      is reponsible for cleaning up memory. */
  virtual void join_delete(Rower* other) { }
};

/*******************************************************************************
 *  Writer::
 *  An interface for iterating through each row of a data frame and modifying it.
 *  The intent is that this class should subclassed and the accept() method be given
 *  a meaningful implementation.
 */
class Writer : public Object {
 public:
  /** This method is called once per row. The row object is on loan and
      should not be retained as it is likely going to be reused in the next
      call. */
  virtual void visit(Row& r) = 0;

  /** Called when Writer execution has completed. */
  virtual bool done() = 0;
};
