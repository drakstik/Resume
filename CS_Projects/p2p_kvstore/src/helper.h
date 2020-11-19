#pragma once
//lang::Cpp

#include <cstdlib>
#include <cstring>
#include <iostream>

#define RESET "\033[0m"

/** Helper class providing some C++ functionality and convenience
 *  functions. This class has no data, constructors, destructors or
 *  virtual functions. Inheriting from it is zero cost.
 */
class Sys {
 public:

  std::string color_code(size_t idx) {
    if (idx == 0) return RESET;
    else return "\033[3" + std::to_string(idx) + "m";
  }

  // Printing functions
  Sys& p(char* c, size_t idx = 0) { 
    std::cout << color_code(idx) << c << RESET; return *this;
  }
  Sys& p(bool c, size_t idx = 0) { 
    std::cout << color_code(idx) << c << RESET; return *this;
  }
  Sys& p(float c, size_t idx = 0) { 
    std::cout << color_code(idx) << c << RESET; return *this;
  }  
  Sys& p(int i, size_t idx = 0) { 
    std::cout << color_code(idx) << i << RESET;  return *this;
  }
  Sys& p(size_t i, size_t idx = 0) { 
    std::cout << color_code(idx) << i << RESET;  return *this;
  }
  Sys& p(const char* c, size_t idx = 0) { 
    std::cout << color_code(idx) << c << RESET;  return *this;
  }
  Sys& p(char c, size_t idx = 0) { 
    std::cout << color_code(idx) << c << RESET;  return *this;
  }
  Sys& pln() { std::cout << "\n";  return *this; }
  Sys& pln(int i, size_t idx = 0) { 
    std::cout << color_code(idx) << i << "\n" << RESET;  return *this;
  }
  Sys& pln(char* c, size_t idx = 0) { 
    std::cout << color_code(idx) << c << "\n" << RESET;  return *this;
  }
  Sys& pln(bool c, size_t idx = 0) { 
    std::cout << color_code(idx) << c << "\n" << RESET;  return *this;
  }  
  Sys& pln(char c, size_t idx = 0) { 
    std::cout << color_code(idx) << c << "\n" << RESET;  return *this;
  }
  Sys& pln(float x, size_t idx = 0) { 
    std::cout << color_code(idx) << x << "\n" << RESET;  return *this;
  }
  Sys& pln(size_t x, size_t idx = 0) { 
    std::cout << color_code(idx) << x << "\n" << RESET;  return *this;
  }
  Sys& pln(const char* c, size_t idx = 0) { 
    std::cout << color_code(idx) << c << "\n" << RESET;  return *this;
  }

  // Copying strings
  char* duplicate(const char* s) {
    char* res = new char[strlen(s) + 1];
    strcpy(res, s);
    return res;
  }
  char* duplicate(char* s) {
    char* res = new char[strlen(s) + 1];
    strcpy(res, s);
    return res;
  }

  // Function to terminate execution with a message
  void exit_if_not(bool b, const char* c) {
    if (b) return;
    if (errno != 0)
      perror(c);
    else
      p("Exit message: ").pln(c);
    exit(-1);
  }
  
  // Definitely fail
//  void FAIL() {
  void myfail(){
    pln("Failing");
    exit(1);
  }

  // Some utilities for lightweight testing
  void OK(const char* m) { pln(m); }
  void t_true(bool p) { if (!p) myfail(); }
  void t_false(bool p) { if (p) myfail(); }
};
