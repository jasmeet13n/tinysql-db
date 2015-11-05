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
    }
    column_names.push_back(node->children[0]->value);
    data_types.push_back(node->children[1]->value == "INT" ? INT : STR20);
    // for CREATE_TABLE_STATEMENT, att_name att_type attribute_type_list
    if(node->children.size() == 3) {
      getAttributeTypeList(node->children[2], column_names, data_types);
    }
  }

  static std::string getTableName(ParseTreeNode* root) {
    return root->children[2]->value;
  }
};

#endif