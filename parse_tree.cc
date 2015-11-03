#ifndef __PARSE_TREE_INCLUDED
#define __PARSE_TREE_INCLUDED

#include <string>
#include <vector>

enum NODE_TYPE {
  CREATE_TABLE_STATEMENT,
  CREATE_LITERAL,
  TABLE_LITERAL,
  DROP_TABLE_STATEMENT,
  DROP_LITERAL
};

class ParseTreeNode {
private:
  enum NODE_TYPE type;
  std::string value;
  std::vector<ParseTreeNode *> children;
public:
  ParseTreeNode(enum NODE_TYPE t) {
    type = t;
  }
};

#endif
