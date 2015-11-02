#include <cctype>
#include <vector>
#include <string>

#include "parse_tree.cc"
#include "tokenizer.cc"
#include "gtest/gtest.h"

class Parser {
private:
  static bool isCreateTableQuery(vector<string>& tokens) {
    if (tokens.size() < 2) {
      return NULL;
    }

    if (toupper(tokens[0]) == "CREATE") {
      if (toupper(tokens[1]) == "TABLE") {
        return true;
      }
    }
    return false;
  }

public:
  static ParseTreeNode* ParseQuery(const string& query) {
    vector<string> tokens;
    tokens = Tokenizer::getTokens(query);
    if (tokens.size() < 3)
      return NULL;

    if (isCreateTableQuery(tokens)) {

    }
  }

};