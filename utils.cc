#ifndef __UTILS_INCLUDED
#define __UTILS_INCLUDED

#include <vector>
#include <string>
#include <iostream>
#include "./StorageManager/Field.h"
#include "parse_tree.cc"

class Utils {
public:
  static void getAttributeTypeList(ParseTreeNode* node, std::vector<std::string>& column_names,
    std::vector<enum FIELD_TYPE>& data_types) {
    if(node->type == NODE_TYPE::CREATE_TABLE_STATEMENT) {
      getAttributeTypeList(node->children[3], column_names, data_types);
      return;
    }
    column_names.push_back(node->children[0]->value);
    data_types.push_back(node->children[1]->value == "INT" ? INT : STR20);
    // for CREATE_TABLE_STATEMENT, att_name att_type attribute_type_list
    if(node->children.size() == 3) {
      getAttributeTypeList(node->children[2], column_names, data_types);
    }
  }

  static void getAttributeList(ParseTreeNode* node, std::vector<std::string>& att_list) {
    if (node->type == NODE_TYPE::INSERT_STATEMENT) {
      getAttributeList(node->children[3], att_list);
      return;
    }
    att_list.push_back(node->children[0]->value);
    if (node->children.size() == 2) {
      getAttributeList(node->children[1], att_list);
    }
  }

  static void getSelectList(ParseTreeNode* node, std::vector<std::string>& select_list) {
    if (node->type == NODE_TYPE::SELECT_STATEMENT) {
      if(node->children[1]->type == NODE_TYPE::DISTINCT_LITERAL) {
        getSelectList(node->children[2]->children[0], select_list);
      }
      else {
        getSelectList(node->children[1]->children[0], select_list);
      }
      return;
    }
    if(node->type == NODE_TYPE::STAR) {
      select_list.push_back(node->value);
      return;
    }
    select_list.push_back(node->children[0]->value);
    if (node->children.size() == 2) {
      getSelectList(node->children[1], select_list);
    }
  }

  static void getTableList(ParseTreeNode* node, std::vector<std::string>& table_list) {
    if (node->type == NODE_TYPE::SELECT_STATEMENT) {
      if(node->children[1]->type == NODE_TYPE::DISTINCT_LITERAL) {
        getTableList(node->children[4], table_list);
      }
      else {
        getTableList(node->children[3], table_list);
      }
      return;
    }
    table_list.push_back(node->children[0]->value);
    if (node->children.size() == 2) {
      getTableList(node->children[1], table_list);
    }
  }

  static void getValueList(ParseTreeNode* node, std::vector<std::string>& value_list) {
    if (node->type == NODE_TYPE::INSERT_STATEMENT) {
      getValueList(node->children[4]->children[1], value_list);
      return;
    }
    value_list.push_back(node->children[0]->value);
    if (node->children.size() == 2) {
      getValueList(node->children[1], value_list);
    }
  }

  static std::string getTableName(ParseTreeNode* root) {
    return root->children[2]->value;
  }

};

#endif
