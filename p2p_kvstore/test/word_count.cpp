#include <iostream>
#include "../src/application.h"

class FileReader : public Writer {
public:
  /** Reads next word and stores it in the row. Actually read the word.
    While reading the word, we may have to re-fill the buffer  */
  void visit(Row & r) override {
    assert(i_ < end_);
    assert(! isspace(buf_[i_]));
    size_t wStart = i_;
    while (true) {
      if (i_ == end_) {
        if (feof(file_)) { ++i_;  break; }
        i_ = wStart;
        wStart = 0;
        fillBuffer_();
      }
      if (isspace(buf_[i_]))  break;
      ++i_;
    }
    buf_[i_] = 0;
    String* word = new String(buf_ + wStart, i_ - wStart);
    r.set(0, word);
    ++i_;
    skipWhitespace_();
  }

  /** Returns true when there are no more words to read.  There is nothing
     more to read if we are at the end of the buffer and the file has
      all been read.     */
  bool done() override { return (i_ >= end_) && feof(file_);  }

  /** Creates the reader and opens the file for reading.  */
  FileReader(char* file) {
    file_ = fopen(file, "r");
    if (file_ == nullptr) std::cout << "Cannot open file " << file;
    buf_ = new char[BUFSIZE + 1]; //  null terminator
    for (int i = 0; i < BUFSIZE + 1; buf_[i++] = 0);
    fillBuffer_();
    skipWhitespace_();
  }

  ~FileReader() { 
    delete[] buf_;
    fclose(file_);
  }

  static const size_t BUFSIZE = 1024;

  /** Reads more data from the file. */
  void fillBuffer_() {
    size_t start = 0;
    // compact unprocessed stream
    if (i_ != end_) {
      start = end_ - i_;
      memcpy(buf_, buf_ + i_, start);
    }
    // read more contents
    end_ = start + fread(buf_+start, sizeof(char), BUFSIZE - start, file_);
    i_ = start;
  }

  /** Skips spaces.  Note that this may need to fill the buffer if the
      last character of the buffer is space itself.  */
  void skipWhitespace_() {
    while (true) {
      if (i_ == end_) {
        if (feof(file_)) return;
        fillBuffer_();
      }
      // if the current character is not whitespace, we are done
      if (!isspace(buf_[i_])) return;
      // otherwise skip it
      ++i_;
    }
  }

  char * buf_;
  size_t end_ = 0;
  size_t i_ = 0;
  FILE * file_;
};
 
 
/****************************************************************************/
class Adder : public Rower {
public:
  SIMap& map_;  // String to Num map;  Num holds an int
 
  Adder(SIMap& map) : map_(map)  {}
 
  bool accept(Row& r) override {
    String* word = r.get_string(0);
    assert(word != nullptr);
    Num* num = map_.contains(*word) ? new Num(map_.get(*word)->v) : new Num();
    num->v++;
    map_.put(*word, num);
    return false;
  }
};
 
/***************************************************************************/
class Summer : public Writer {
public:
  SIMap& map_;
  size_t i = 0;
  size_t j = 0;
  size_t seen = 0;
 
  Summer(SIMap& map) : map_(map) {}
 
  void next() {
    if (i == map_.capacity_ ) return;
    if ( j < map_.items_[i].keys_.size() ) {
      j++;
    } else {
      ++i;
      j = 0;
      while( i < map_.capacity_ && map_.items_[i].keys_.size() == 0 )  i++;
    }
  }
 
  String* k() {
    if (i==map_.capacity_ || j == map_.items_[i].keys_.size()) 
      return nullptr;
    return (String*) (map_.items_[i].keys_.get(j));
  }
 
  size_t v() {
    if (i == map_.capacity_ || j == map_.items_[i].keys_.size()) {
      assert(false); return 0;
    }
    return ((Num*)(map_.items_[i].vals_.get(j)))->v;
  }
 
  void visit(Row& r) {
    if (!k()) next();
    String* key = k()->clone();
    size_t value = v();
    r.set(0, key);
    r.set(1, (int) value);
    ++seen;
    next();
  }
 
  bool done() { return seen == map_.size(); }
};
 
/****************************************************************************
 * Calculate a word count for given file:
 *   1) read the data (single node)
 *   2) produce word counts per homed chunks, in parallel
 *   3) combine the results
 **********************************************************author: pmaj ****/
class WordCount: public Application {
public:
  Key in;
  Key* map;
  KeyBuff kbuf;
  SIMap all;
  char* file;
  size_t num_nodes;
 
  WordCount(size_t idx, size_t num_nodes, char* file):
    Application(idx, num_nodes), in("data", 0), map(new Key("wc-map-",0)), kbuf(map), file(file),
    num_nodes(num_nodes) { 
      run_();    
  }

  ~WordCount() { delete map; }
 
  /** The master nodes reads the input, then all of the nodes count. */
  void run_() override {
    if (this_node() == 0) {
      FileReader fr(file);
      delete DataFrame::fromVisitor(&in, &kd_, "S", fr);
    }
    local_count();
    reduce();
  }
 
  /** Returns a key for given node.  These keys are homed on master node
   *  which then joins them one by one. */
  Key* mk_key(size_t idx) {
      Key * k = kbuf.c(idx).get(0);
      p("Node ", this_node()).p(this_node(), this_node()).p(": Created key ", this_node())
        .pln(k->get_keystring()->c_str(), this_node());
      return k;
  }
 
  /** Compute word counts on the local node and build a data frame. */
  void local_count() {
    DataFrame* words = (kd_.wait_and_get(in));
    p("Node ", this_node()).p(this_node(), this_node())
      .pln(": starting local count...", this_node());
    SIMap map;
    Adder add(map);
    words->local_map(add);
    delete words;
    Summer cnt(map);
    Key* local = mk_key(this_node());
    delete DataFrame::fromVisitor(local, &kd_, "SI", cnt);
    delete local;
  }
 
  /** Merge the data frames of all nodes */
  void reduce() {
    if (this_node() != 0) return;
    pln("Node 0: reducing counts...", this_node());
    SIMap map;
    Key* own = mk_key(0);
    merge(kd_.get(*own), map);
    for (size_t i = 1; i < num_nodes; ++i) { // merge other nodes
      Key* ok = mk_key(i);
      merge(kd_.wait_and_get(*ok), map);
      delete ok;
    }
    p("Different words: ", this_node()).pln(map.size(), this_node());
    delete own;
    sleep(1);
    done();
  }
 
  void merge(DataFrame* df, SIMap& m) {
    Adder add(m);
    df->map(add);
    delete df;
  }
}; // WordcountDemo

int main(int argc, char** argv) {
  Sys s;
  size_t node_idx;
  size_t num_nodes;
  char* filename;
  int c;

  while ((c = getopt(argc, argv, "i:n:f:")) != -1) {
    switch (c) {
      case 'i':
        node_idx = atoi(optarg);
        break;
      case 'n':
        num_nodes = atoi(optarg);
        break;
      case 'f':
        filename = optarg;
        break;
      default:
        fprintf(stderr, "Invalid command line args.");
        return 1;
    }
  }

  WordCount(node_idx, num_nodes, filename);
}