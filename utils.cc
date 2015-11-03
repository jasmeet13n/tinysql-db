#ifndef __UTILS_INCLUDED
#define __UTILS_INCLUDED

#include <vector>
#include <string>
#include "./StorageManager/Field.h"

std::vector<std::string> getColumnNames(ParseTreeNode* root) {
  std::vector<std::string> result;
  if(type == CREATE_TABLE_STATEMENT) {
    // children[4] -> attribute_type_list
    getColumnNamesRecursively(result, root->children[4]);
  }
  return result;
}

std::vector<enum FIELD_TYPE> getDataTypes(ParseTreeNode* root) {
  std::vector<enum FIELD_TYPE> result;
  if(type == CREATE_TABLE_STATEMENT) {
    // children[4] -> attribute_type_list
    getDataTypesRecursively(result, root->children[4]);
  }
  return result;
}

std::string getRelationName(ParseTreeNode* root) {
  return root->children[2]->value;
}

void getColumnNamesRecursively(std::vector<std::string>& result, ParseTreeNode* node) {
  result.push_back(node->children[0]->value);
  // for CREATE_TABLE_STATEMENT, att_name att_type attribute_type_list
  if(node->children.size() == 3) {
    getColumnNamesRecursively(result, node->children[2]);
  }
}

void getDataTypesRecursively(std::vector<enum FIELD_TYPE>& result, ParseTreeNode* node) {
  result.push_back(node->children[1]->value == "INT" ? INT : STR20);
  // for CREATE_TABLE_STATEMENT, att_name att_type attribute_type_list
  if(node->children.size() == 3) {
    getDataTypesRecursively(result, node->children[2]);
  }
}

#endif