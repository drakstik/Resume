#include "../src/application.h"

/**
 * The input data is a processed extract from GitHub.
 *
 * projects:  I x S   --  The first field is a project id (or pid).
 *                    --  The second field is that project's name.
 *                    --  In a well-formed dataset the largest pid
 *                    --  is equal to the number of projects.
 *
 * users:    I x S    -- The first field is a user id, (or uid).
 *                    -- The second field is that user's name.
 *
 * commits: I x I x I -- The fields are pid, uid, uid', each row represent
 *                    -- a commit to project pid, written by user uid
 *                    -- and committed by user uid',
 **/

/**************************************************************************
 * A bit set contains size() booleans that are initialize to false and can
 * be set to true with the set() method. The test() method returns the
 * value. Does not grow.
 ************************************************************************/
class Set {
public:  
  bool* vals_;  // owned; data
  size_t size_; // number of elements
  size_t true_; // number of true elements

  /** Creates a set of the same size as the dataframe. */ 
  Set(DataFrame* df) : Set(df->nrows()) {}

  /** Creates a set of the given size. */
  Set(size_t sz) :  vals_(new bool[sz]), size_(sz), true_(0) {
    for(size_t i = 0; i < size_; i++)
      vals_[i] = false; 
  }

  ~Set() { delete[] vals_; }

  /** Add idx to the set. If idx is out of bound, ignore it.  Out of bound
   *  values can occur if there are references to pids or uids in commits
   *  that did not appear in projects or users.
   */
  void set(size_t idx) {
    if (idx >= size_ ) return; // ignoring out of bound writes
    vals_[idx] = true;
    true_++;
  }

  /** Is idx in the set?  See comment for set(). */
  bool test(size_t idx) {
    if (idx >= size_) return true; // ignoring out of bound reads
    return vals_[idx];
  }

  size_t size() { return size_; }

  size_t num_true() { return true_; }

  /** Performs set union in place. */
  void union_(Set& from) {
    for (size_t i = 0; i < from.size_; i++) 
      if (from.test(i))
	      set(i);
  }
};


/*******************************************************************************
 * A SetUpdater is a reader that gets the first column of the data frame and
 * sets the corresponding value in the given set.
 ******************************************************************************/
class SetUpdater : public Rower {
public:
  Set& set_; // set to update
  
  SetUpdater(Set& set): set_(set) {}

  /** Assume a row with at least one column of type I. Assumes that there
   * are no missing. Reads the value and sets the corresponding position.
   * The return value is irrelevant here. */
  bool accept(Row & row) { set_.set(row.get_int(0));  return false; }
};

/*****************************************************************************
 * A SetWriter copies all the values present in the set into a one-column
 * dataframe. The data contains all the values in the set. The dataframe has
 * at least one integer column.
 ****************************************************************************/
class SetWriter: public Writer {
public:
  Set& set_; // set to read from
  int i_ = 0;  // position in set

  SetWriter(Set& set): set_(set) { }

  /** Skip over false values and stop when the entire set has been seen */
  bool done() {
    while (i_ < set_.size_ && set_.test(i_) == false) ++i_;
    return i_ == set_.size_;
  }

  void visit(Row & row) { row.set(0, i_++); }
};

/***************************************************************************
 * The ProjectTagger is a reader that is mapped over commits, and marks all
 * of the projects to which a collaborator of Linus committed as an author.
 * The commit dataframe has the form:
 *    pid x uid x uid
 * where the pid is the identifier of a project and the uids are the
 * identifiers of the author and committer. If the author is a collaborator
 * of Linus, then the project is added to the set. If the project was
 * already tagged then it is not added to the set of newProjects.
 *************************************************************************/
class ProjectsTagger : public Rower {
public:
  Set& uSet; // set of collaborator 
  Set& pSet; // set of projects of collaborators
  Set newProjects;  // newly tagged collaborator projects

  ProjectsTagger(Set& uSet, Set& pSet, DataFrame* proj):
    uSet(uSet), pSet(pSet), newProjects(proj) {}

  /** The data frame must have at least two integer columns. The newProject
   * set keeps track of projects that were newly tagged (they will have to
   * be communicated to other nodes). */
  bool accept(Row & row) override {
    int pid = row.get_int(0);
    int uid = row.get_int(1);
    if (uSet.test(uid)) 
      if (!pSet.test(pid)) {
    	  pSet.set(pid);
        newProjects.set(pid);
      }
    return false;
  }
};

/***************************************************************************
 * The UserTagger is a reader that is mapped over commits, and marks all of
 * the users which commmitted to a project to which a collaborator of Linus
 * also committed as an author. The commit dataframe has the form:
 *    pid x uid x uid
 * where the pid is the idefntifier of a project and the uids are the
 * identifiers of the author and committer. 
 *************************************************************************/
class UsersTagger : public Rower {
public:
  Set& pSet;
  Set& uSet;
  Set newUsers;

  UsersTagger(Set& pSet,Set& uSet, DataFrame* users):
    pSet(pSet), uSet(uSet), newUsers(users->nrows()) { }

  bool accept(Row & row) override {
    int pid = row.get_int(0);
    int uid = row.get_int(1);
    if (pSet.test(pid)) 
      if (!uSet.test(uid)) {
        uSet.set(uid);
        newUsers.set(uid);
      }
    return false;
  }
};

/*************************************************************************
 * This computes the collaborators of Linus Torvalds.
 * is the linus example using the adapter.  And slightly revised
 *   algorithm that only ever trades the deltas.
 **************************************************************************/
class Linus : public Application {
public:
  int DEGREES = 7;  // How many degrees of separation form linus?
  int LINUS = 4967;   // The uid of Linus (offset in the user df)
  const char* PROJ = "data/projects.ltgt";
  const char* USER = "data/users.ltgt";
  const char* COMM = "data/commits.ltgt";
  DataFrame* projects; //  pid x project name
  DataFrame* users;  // uid x user name
  DataFrame* commits;  // pid x uid x uid 
  Set* uSet; // Linus' collaborators
  Set* pSet; // projects of collaborators
  size_t num_nodes;
  char* len; // number of byes to read from the datafiles
  char* key_str; // temporary buffer that holds key strings
  Key* linus_key; // the key to the df containing Linus' id

  Linus(size_t idx, size_t nodes, char* len): Application(idx, nodes), num_nodes(nodes), len(len),
    linus_key(new Key("users-0-0", 0)) {
    run_();
  }

  ~Linus() {
    delete projects;
    delete users;
    delete commits;
    delete linus_key;
    delete uSet;
    delete pSet;
  }

  /** Compute DEGREES of Linus.  */
  void run_() override {
    readInput();
    for (size_t i = 0; i < DEGREES; i++) step(i);
    if (this_node() == 0) {
      sleep(3);
      done();
    }
  }

  /** Node 0 reads three files, cointainng projects, users and commits, and
   *  creates thre dataframes. All other nodes wait and load the three
   *  dataframes. Once we know the size of users and projects, we create
   *  sets of each (uSet and pSet). We also output a data frame with a the
   *  'tagged' users. At this point the dataframe consists of only
   *  Linus. **/
  void readInput() {
    Key pK("projs", 0);
    Key uK("usrs", 0);
    Key cK("comts", 0);
    if (this_node() == 0) {
      pln("Reading...");
      projects = DataFrame::fromFile(PROJ, &pK, &kd_, len);
      p("    ").p(projects->nrows()).pln(" projects");
      users = DataFrame::fromFile(USER, &uK, &kd_, len);
      p("    ").p(users->nrows()).pln(" users");
      commits = DataFrame::fromFile(COMM, &cK, &kd_, len);
      p("    ").p(commits->nrows()).pln(" commits");
      // This dataframe contains the id of Linus.
      delete DataFrame::fromIntScalar(linus_key, &kd_, LINUS);
    } else {
      projects = dynamic_cast<DataFrame*>(kd_.wait_and_get(pK));
      users = dynamic_cast<DataFrame*>(kd_.wait_and_get(uK));
      commits = dynamic_cast<DataFrame*>(kd_.wait_and_get(cK));
    }
    uSet = new Set(users);
    pSet = new Set(projects);
  }

 /** Performs a step of the linus calculation. It operates over the three
  *  datafrrames (projects, users, commits), the sets of tagged users and
  *  projects, and the users added in the previous round. */
  void step(int stage) {
    p("Stage ", this_node()).pln(stage, this_node());
    // Key of the shape: users-stage-0
    key_str = StrBuff("users-").c(stage).c("-0").c_str();
    Key uK(key_str, 0);
    delete[] key_str;
    // A df with all the users added on the previous round
    DataFrame* newUsers = dynamic_cast<DataFrame*>(kd_.wait_and_get(uK));    
    Set delta(users);
    SetUpdater upd(delta);  
    newUsers->map(upd); // all of the new users are copied to delta.
    delete newUsers;
    ProjectsTagger ptagger(delta, *pSet, projects);
    commits->local_map(ptagger); // marking all projects touched by delta
    merge(ptagger.newProjects, "projects-", stage);
    pSet->union_(ptagger.newProjects); // 
    UsersTagger utagger(ptagger.newProjects, *uSet, users);
    commits->local_map(utagger);
    merge(utagger.newUsers, "users-", stage + 1);
    uSet->union_(utagger.newUsers);
    p("    after stage ", this_node()).p(stage, this_node()).pln(":", this_node());
    p("        tagged projects: ", this_node()).pln(pSet->num_true(), this_node());
    p("        tagged users: ", this_node()).pln(uSet->num_true(), this_node());
  }

  /** Gather updates to the given set from all the nodes in the systems.
   * The union of those updates is then published as dataframe.  The key
   * used for the otuput is of the form "name-stage-0" where name is either
   * 'users' or 'projects', stage is the degree of separation being
   * computed.
   */ 
  void merge(Set& set, char const* name, int stage) {
    if (this_node() == 0) {
      for (size_t i = 1; i < num_nodes; ++i) {
        key_str = StrBuff(name).c(stage).c("-").c(i).c_str();
        Key nK(key_str, 0);
        delete[] key_str;
        DataFrame* delta = dynamic_cast<DataFrame*>(kd_.wait_and_get(nK));
        p("    received delta of ", this_node()).p(delta->nrows())
          .p(" elements from node ", this_node()).pln(i);
        SetUpdater upd(set);
        delta->map(upd);
        delete delta;
      }
      p("    storing ", this_node()).p(set.num_true(), this_node())
        .pln(" merged elements", this_node());
      SetWriter writer(set);
      key_str = StrBuff(name).c(stage).c("-0").c_str();
      Key k(key_str, 0);
      delete[] key_str;
      delete DataFrame::fromVisitor(&k, &kd_, "I", writer);
    } else {
      p("    sending ", this_node()).p(set.num_true(), this_node())
        .pln(" elements to master node", this_node());
      SetWriter writer(set);
      key_str = StrBuff(name).c(stage).c("-").c(this_node()).c_str();
      Key k(key_str, 0);
      delete[] key_str;
      delete DataFrame::fromVisitor(&k, &kd_, "I", writer);
      key_str = StrBuff(name).c(stage).c("-0").c_str();
      Key mK(key_str, 0);
      delete[] key_str;
      DataFrame* merged = dynamic_cast<DataFrame*>(kd_.wait_and_get(mK));
      p("    receiving ", this_node()).p(merged->nrows(), this_node())
        .pln(" merged elements", this_node());
      SetUpdater upd(set);
      merged->map(upd);
      delete merged;
    }
  }
}; // Linus

int main(int argc, char** argv) {
  Sys s;
  size_t node_idx = 0;
  size_t num_nodes = 1;
  // number of byes to read from the datafiles
  // defaults to 0, in which case the entire files will be read
  char* len = 0;
  int c;

  while ((c = getopt(argc, argv, "i:n:l:")) != -1) {
    switch (c) {
      case 'i':
        node_idx = atoi(optarg);
        break;
      case 'n':
        num_nodes = atoi(optarg);
        break;
      case 'l':
        len = optarg;
        break;
      default:
        fprintf(stderr, "Invalid command line args.");
        return 1;
    }
  }

  Linus(node_idx, num_nodes, len);
}