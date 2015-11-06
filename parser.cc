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

  static bool isDeleteTableQuery(std::vector<std::string>& tokens) {
    if (tokens.size() != 3) {
      return false;
    }
    std::string t0 = boost::to_upper_copy<std::string>(tokens[0]);
    if (t0 == "DROP") {
      std::string t1 = boost::to_upper_copy<std::string>(tokens[1]);
      if (t1 == "TABLE") {
        return true;
      }
    }
    return false;
  }

  static bool isInsertIntoTableQuery(std::vector<std::string>& tokens) {
    if (tokens.size() < 3) {
      return false;
    }
    std::string t0 = boost::to_upper_copy<std::string>(tokens[0]);
    if (t0 == "INSERT") {
      std::string t1 = boost::to_upper_copy<std::string>(tokens[1]);
      if (t1 == "INTO") {
        return true;
      }
    }
    return false;
  }

  static bool isSelectQuery(std::vector<std::string>& tokens, int start_index = 0) {
    if (tokens.size() < 3) {
      return false;
    }
    std::string t0 = boost::to_upper_copy<std::string>(tokens[start_index + 0]);
    if (t0 == "SELECT") {
      return true;
    }
    return false;
  }

  static ParseTreeNode* getAttributeTypeList(
      std::vector<std::string>& tokens, int start_index) {
    std::string att_name = tokens[start_index];
    std::string att_type = tokens[start_index + 1];

    ParseTreeNode* att_type_list = new ParseTreeNode(NODE_TYPE::ATTRIBUTE_TYPE_LIST, "attribute_type_list");
    (att_type_list->children).push_back(new ParseTreeNode(NODE_TYPE::ATTRIBUTE_NAME, att_name));
    (att_type_list->children).push_back(new ParseTreeNode(NODE_TYPE::ATTRIBUTE_DATA_TYPE, att_type));

    if (tokens[start_index + 2] == ",") {
      (att_type_list->children).push_back(getAttributeTypeList(tokens, start_index + 3));
    }

    return att_type_list;
  }

  static ParseTreeNode* getAttributeList(
      std::vector<std::string>& tokens, int start_index) {
    std::string att_name = tokens[start_index];

    ParseTreeNode* att_list = new ParseTreeNode(NODE_TYPE::ATTRIBUTE_LIST, "attribute_list");
    (att_list->children).push_back(new ParseTreeNode(NODE_TYPE::ATTRIBUTE_NAME, att_name));

    if (tokens[start_index + 1] == ",") {
      (att_list->children).push_back(getAttributeList(tokens, start_index + 2));
    }

    return att_list;
  }

  static ParseTreeNode* getValueList(
      std::vector<std::string>& tokens, int start_index) {
    ParseTreeNode* value_list = new ParseTreeNode(NODE_TYPE::VALUE_LIST, "value_list");

    std::string value = tokens[start_index];
    (value_list->children).push_back(new ParseTreeNode(NODE_TYPE::VALUE, value));

    if (tokens[start_index + 1] == ",") {
      (value_list->children).push_back(getValueList(tokens, start_index + 2));
    }

    return value_list;
  }

  static ParseTreeNode* getInsertTuples(
      std::vector<std::string>& tokens, int start_index) {
    std::string values_literal = boost::to_upper_copy<std::string>(tokens[start_index]);
    // if not values get select statement

    ParseTreeNode* insert_tuples = new ParseTreeNode(NODE_TYPE::INSERT_TUPLES, "insert_tuples");

    (insert_tuples->children).push_back(new ParseTreeNode(NODE_TYPE::VALUES_LITERAL, "VALUES"));
    (insert_tuples->children).push_back(getValueList(tokens, start_index + 2));

    return insert_tuples;
  }

  static ParseTreeNode* getSelectSublist(std::vector<std::string>& tokens, int start_index) {
    std::string value = tokens[start_index];
    if(value == "*")
      return new ParseTreeNode(NODE_TYPE::STAR, "*");

    ParseTreeNode* sub_list = new ParseTreeNode(NODE_TYPE::SELECT_SUBLIST, "select_sublist");
    (sub_list->children).push_back(new ParseTreeNode(NODE_TYPE::COLUMN_NAME, value));

    if (start_index + 1 < tokens.size() && tokens[start_index + 1] == ",") {
      (sub_list->children).push_back(getSelectSublist(tokens, start_index + 2));
    }

    return sub_list;
  }

  static ParseTreeNode* getTableList(std::vector<std::string>& tokens, int start_index) {
    ParseTreeNode* table_list = new ParseTreeNode(NODE_TYPE::TABLE_LIST, "table_list");

    std::string value = tokens[start_index];
    (table_list->children).push_back(new ParseTreeNode(NODE_TYPE::TABLE_NAME, value));

    if (start_index < tokens.size() - 1 && tokens[start_index + 1] == ",") {
      (table_list->children).push_back(getTableList(tokens, start_index + 2));
    }

    return table_list;
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

  static ParseTreeNode* getDropTableTree(std::vector<std::string>& tokens) {
    ParseTreeNode* root = new ParseTreeNode(NODE_TYPE::DROP_TABLE_STATEMENT, "drop_statement");
    (root->children).push_back(new ParseTreeNode(NODE_TYPE::DROP_LITERAL, "DROP"));
    (root->children).push_back(new ParseTreeNode(NODE_TYPE::TABLE_LITERAL, "TABLE"));

    // add relation name child
    (root->children).push_back(new ParseTreeNode(NODE_TYPE::TABLE_NAME, tokens[2]));

    return root;
  }

  static ParseTreeNode* getInsertIntoTableTree(std::vector<std::string>& tokens) {
    ParseTreeNode* root = new ParseTreeNode(NODE_TYPE::INSERT_STATEMENT, "insert_statement");
    (root->children).push_back(new ParseTreeNode(NODE_TYPE::INSERT_LITERAL, "INSERT"));
    (root->children).push_back(new ParseTreeNode(NODE_TYPE::INTO_LITERAL, "INTO"));

    // add relation name child
    (root->children).push_back(new ParseTreeNode(NODE_TYPE::TABLE_NAME, tokens[2]));

    // add attribute list
    (root->children).push_back(getAttributeList(tokens, 4));

    int values_start = 4;
    while (tokens[values_start] != ")") {
      values_start++;
    }
    values_start++;

    //std::cout << "values start at: " << values_start << " token: " << tokens[values_start] << std::endl;
    (root->children).push_back(getInsertTuples(tokens, values_start));

    return root;
  }

  static ParseTreeNode* getSelectTree(std::vector<std::string>& tokens, int start_index = 0) {
    int distinct_index = 0;
    ParseTreeNode* root = new ParseTreeNode(NODE_TYPE::SELECT_STATEMENT, "select_statement");
    (root->children).push_back(new ParseTreeNode(NODE_TYPE::SELECT_LITERAL, "SELECT"));
    if(tokens[1 + start_index] == "DISTINCT") {
      (root->children).push_back(new ParseTreeNode(NODE_TYPE::DISTINCT_LITERAL, "DISTINCT"));
      distinct_index++;
    }
    (root->children).push_back(new ParseTreeNode(NODE_TYPE::SELECT_LIST, "select_list"));
    ((root->children[root->children.size() - 1])->children).push_back(getSelectSublist(
      tokens, 1 + start_index + distinct_index));
    (root->children).push_back(new ParseTreeNode(NODE_TYPE::FROM_LITERAL, "FROM"));
    int table_list_start = start_index;
    while (tokens[table_list_start] != "FROM") {
      table_list_start++;
    }
    table_list_start++;
    (root->children).push_back(getTableList(tokens, table_list_start));
    int where_start = table_list_start;
    int order_start = table_list_start;
    bool where_present = false;
    bool order_by_present = false;
    while (where_start < tokens.size()) {
      if(tokens[where_start] == "WHERE") {
        where_present = true;
        break;
      }
      where_start++;
    }

    while (order_start < tokens.size()) {
      if(tokens[order_start] == "ORDER") {
        order_by_present = true;
        break;
      }
      order_start++;
    }

    if(where_present) {
      where_start++;
      (root->children).push_back(new ParseTreeNode(NODE_TYPE::WHERE_LITERAL, "WHERE"));
      //make where_clause
      std::string where_clause = "";
      while(where_start < tokens.size() && tokens[where_start] != "ORDER") {
        where_clause = where_clause + tokens[where_start++] + " ";
      }
      ((root->children[root->children.size() - 1])->children).push_back(
        new ParseTreeNode(NODE_TYPE::WHERE_CLAUSE, where_clause));
    }

    if(order_by_present) {
      (root->children).push_back(new ParseTreeNode(NODE_TYPE::ORDER_LITERAL, "ORDER"));
      (root->children).push_back(new ParseTreeNode(NODE_TYPE::BY_LITERAL, "BY"));
      (root->children).push_back(new ParseTreeNode(NODE_TYPE::COLUMN_NAME, tokens[order_start + 2]));
    }
    return root;
  }

public:
  static ParseTreeNode* parseQuery(const std::string& query) {
    std::vector<std::string> tokens;
    tokens = Tokenizer::getTokens(query);
    // make first token toUpper

    if (tokens.size() < 3)
      return nullptr;

    if (isCreateTableQuery(tokens)) {
      ParseTreeNode* ans = getCreateTableTree(tokens);
      ParseTreeNode::printParseTree(ans);
      return ans;
    } else if (isDeleteTableQuery(tokens)) {
      ParseTreeNode* ans = getDropTableTree(tokens);
      //ParseTreeNode::printParseTree(ans);
      return ans;
    } else if (isInsertIntoTableQuery(tokens)) {
      ParseTreeNode* ans = getInsertIntoTableTree(tokens);
      //ParseTreeNode::printParseTree(ans);
      return ans;
    } else if (isSelectQuery(tokens)) {
      ParseTreeNode* ans = getSelectTree(tokens);
      ParseTreeNode::printParseTree(ans);
      return ans;
    }
    return nullptr;
  }
};

#endif
