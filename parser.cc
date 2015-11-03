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

  static ParseTreeNode* getAttributeTypeList(
      std::vector<std::string>& tokens, int start_index) {
    std::string att_name = tokens[start_index];
    std::string att_type = tokens[start_index + 1];

    ParseTreeNode* att_type_list = new ParseTreeNode(NODE_TYPE::ATTRIBUTE_TYPE_LIST, "attribute_list");
    (att_type_list->children).push_back(new ParseTreeNode(NODE_TYPE::ATTRIBUTE_NAME, att_name));
    (att_type_list->children).push_back(new ParseTreeNode(NODE_TYPE::ATTRIBUTE_DATA_TYPE, att_type));

    if (tokens[start_index + 2] == ",") {
      (att_type_list->children).push_back(getAttributeTypeList(tokens, start_index + 3));
    }

    return att_type_list;
  }

  static ParseTreeNode* getCreateTableTree(std::vector<std::string>& tokens) {
    ParseTreeNode* root = new ParseTreeNode(NODE_TYPE::CREATE_TABLE_STATEMENT, "create_statement");
    (root->children).push_back(new ParseTreeNode(NODE_TYPE::CREATE_LITERAL, "CREATE"));
    (root->children).push_back(new ParseTreeNode(NODE_TYPE::TABLE_LITERAL, "TABLE"));

    // add relation name child
    (root->children).push_back(new ParseTreeNode(NODE_TYPE::TABLE_NAME, tokens[2]));

    (root->children).push_back(getAttributeTypeList(tokens, 4));

    return root;
  }

public:
  static ParseTreeNode* parseQuery(const std::string& query) {
    std::vector<std::string> tokens;
    tokens = Tokenizer::getTokens(query);
    if (tokens.size() < 3)
      return nullptr;

    if (isCreateTableQuery(tokens)) {
      ParseTreeNode* ans = getCreateTableTree(tokens);
      ParseTreeNode::printParseTree(ans);
      return ans;

    }
    return nullptr;
  }

};

#endif
