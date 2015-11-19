#ifndef __LOGICAL_QUERY_TREE_INCLUDED
#define __LOGICAL_QUERY_TREE_INCLUDED

#include <string>
#include <vector>

enum LQT_NODE_TYPE {
  PROJECTION,
  SELECTION,
  CROSS_JOIN,
  NATURAL_JOIN,
  REMOVE_DUPS,
  TABLE_SCAN
};

class LQueryTreeNode {
private:
public:
  enum LQT_NODE_TYPE type;
  std::string value; // This can be table_name etc.
  std::vector<std::string> att_list; // This can be list of column_names
  std::vector<LQueryTreeNode *> children;
  ParseTreeNode* condition; // In case of selection with condition

  LQueryTreeNode(enum LQT_NODE_TYPE t) {
    type = t;
  }

  ~LQueryTreeNode() {
    for (int i = 0; i < children.size(); ++i) {
      delete children[i];
    }
  }

};

#endif
