#ifndef __PARSE_TREE_INCLUDED
#define __PARSE_TREE_INCLUDED

#include <string>
#include <vector>

enum DATA_TYPE {
  INT,
  SRT20
};

enum NODE_TYPE {
  CREATE_TABLE_STATEMENT,
  CREATE_LITERAL,
  TABLE_LITERAL,
  DROP_TABLE_STATEMENT,
  DROP_LITERAL,
  TABLE_NAME,
  ATTRIBUTE_TYPE_LIST,
  ATTRIBUTE_NAME,
  ATTRIBUTE_DATA_TYPE
};

class ParseTreeNode {
private:
public:
  enum NODE_TYPE type;
  std::string value;
  std::vector<ParseTreeNode *> children;

  ParseTreeNode(enum NODE_TYPE t) {
    type = t;
  }

  static void printParseTree(ParseTreeNode* root, int level = 0) {
    if (root == NULL) {
      return;
    }
    for (int i = 0; i < level; ++i)
      std::cout << "-";
    std::cout << root->value << std::endl;
    for (int i = 0; i < (root->children).size(); ++i) {
      printParseTree(root->children[i], level + 2);
    }
  }
};

#endif
