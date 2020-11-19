//lang::CwC

#pragma once

#include "kdstore.h"

/**
 * Abstract class outlining an application that uses the eau2 system.
 * 
 * @author Spencer LaChance <lachance.s@northeastern.edu>
 * @author David Mberingabo <mberingabo.d@husky.neu.edu>
 */
class Application : public Object {
public:
    // The index of the current node running the application
    size_t idx_;
    // The current node's K/D store (takes in keys and returns DataFrames)
    KDStore kd_;

    /** Constructor */
    Application(size_t idx, size_t nodes) : idx_(idx), kd_(idx, nodes) { }

    /** Runs the application on the current node. */
    virtual void run_() { }

    /** Getter for the current node index. */
    size_t this_node() { return idx_; }

    /** Called when the application has finished its execution. */
    void done() { kd_.done(); }
};