#ifndef __UTILS_INCLUDED
#define __UTILS_INCLUDED

#include <vector>
#include <string>
#include <iostream>
#include "./StorageManager/Field.h"
#include "parse_tree.cc"
#include "logical_query_tree.cc"

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

  static LQueryTreeNode* createLogicalQueryTree(ParseTreeNode* pt_root) {
    // I think we need Logical Query Plan only if there are multiple tables
    // Otherwise directly use parsetree to output

    // create PROJECTION
    LQueryTreeNode* projection_root = createProjection(pt_root);

    // create SELECTION
    LQueryTreeNode* selection_root = createSelection(pt_root);

    //create CROSS JOIN
    LQueryTreeNode* cross_join_root = createCrossJoin(pt_root);

    //create DUP REMOVAL
    LQueryTreeNode* dup_remove_root = createDupRemove(pt_root);

    LQueryTreeNode* current = projection_root;

    //add dup_remove root
    if(dup_remove_root != nullptr) {
      current->children.push_back(dup_remove_root);
      current = current->children[current->children.size() - 1];
    }
    
    //add selection root
    current->children.push_back(selection_root);
    current = current->children[current->children.size() - 1];

    //add cross_join_root
    current->children.push_back(cross_join_root);

    //return root
    return projection_root;
  }

  static LQueryTreeNode* createDupRemove(ParseTreeNode* pt_root) {
    if(pt_root->children[1]->type == NODE_TYPE::DISTINCT_LITERAL)
      return new LQueryTreeNode(LQT_NODE_TYPE::DUP_REMOVE);
    return nullptr;
  }

  static LQueryTreeNode* createProjection(ParseTreeNode* pt_root) {
    LQueryTreeNode* lqt_node = new LQueryTreeNode(LQT_NODE_TYPE::PROJECTION);
    Utils::getSelectList(pt_root, lqt_node->att_list);
    return lqt_node;
  }

  static LQueryTreeNode* createSelection(ParseTreeNode* pt_root) {
    int index = pt_root->children[1]->type == NODE_TYPE::DISTINCT_LITERAL ? 6 : 5;
    LQueryTreeNode* lqt_node = new LQueryTreeNode(LQT_NODE_TYPE::SELECTION);
    if(pt_root->children.size() >= index-1 && pt_root->children[index-1]->type == NODE_TYPE::WHERE_LITERAL) {
      lqt_node->condition = pt_root->children[index];
    }
    return lqt_node;
  }

  // Helper for createCrossJoin
  static void getCrossJoin(LQueryTreeNode* lqt_node, ParseTreeNode* pt_node) {
    //create right deep tree
    lqt_node->children.push_back(new LQueryTreeNode(LQT_NODE_TYPE::TABLE_SCAN));
    lqt_node->children[lqt_node->children.size() - 1]->value = pt_node->children[0]->value;
    if(pt_node->children[1]->children.size() == 1) {
      lqt_node->children.push_back(new LQueryTreeNode(LQT_NODE_TYPE::TABLE_SCAN));
      lqt_node->children[lqt_node->children.size() - 1]->value = pt_node->children[1]->children[0]->value;
      return;
    }
    lqt_node->children.push_back(new LQueryTreeNode(LQT_NODE_TYPE::CROSS_JOIN));
    getCrossJoin(lqt_node->children[1], pt_node->children[1]);
  }

  // Cross Join SubTree
  static LQueryTreeNode* createCrossJoin(ParseTreeNode* pt_root) {
    LQueryTreeNode* lqt_node = new LQueryTreeNode(LQT_NODE_TYPE::CROSS_JOIN);
    int index = pt_root->children[1]->type == NODE_TYPE::DISTINCT_LITERAL ? 4 : 3;
    getCrossJoin(lqt_node, pt_root->children[index]);
    return lqt_node;
  }

  static bool hasWhereCondition(ParseTreeNode* root) {
    int index = root->children[1]->type == NODE_TYPE::DISTINCT_LITERAL ? 4 : 3;
    if (root->children.size() > 5 && root->children[index+1]->type == NODE_TYPE::WHERE_LITERAL) {
      return true;
    }
    return false;
  }

};

#endif
