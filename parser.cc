#ifndef __PARSER_INCLUDED
#define __PARSER_INCLUDED

#include <vector>
#include <string>
#include <algorithm>
#include <gtest/gtest.h>
#include <boost/algorithm/string.hpp>

#include "parse_tree.cc"
#include "tokenizer.cc"

class Parser {
private:
  static bool isCreateTableQuery(std::vector<std::string>& tokens) {
    if (tokens.size() < 2) {
      return false;
    }
    std::string t0 = boost::to_upper_copy<std::string>(tokens[0]);
    if (t0 == "CREATE") {
      std::string t1 = boost::to_upper_copy<std::string>(tokens[1]);
      if (t1 == "TABLE") {
        return true;
      }
    }
    return false;
  }

  static ParseTreeNode* getCreateTableTree(std::vector<std::string>& tokens) {
    ParseTreeNode* root = new ParseTreeNode(NODE_TYPE::CREATE_TABLE_STATEMENT);
    root->children.push_back(new ParseTreeNode(NODE_TYPE::CREATE_LITERAL));
    root->children.push_back(new ParseTreeNode(NODE_TYPE::TABLE_LITERAL));

    return nullptr;
  }

public:
  static ParseTreeNode* parseQuery(const std::string& query) {
    std::vector<std::string> tokens;
    tokens = Tokenizer::getTokens(query);
    if (tokens.size() < 3)
      return nullptr;

    if (isCreateTableQuery(tokens)) {
      return getCreateTableTree(tokens);
    }
    return nullptr;
  }

};

#endif
