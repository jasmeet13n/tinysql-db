enum NODE_TYPE {create-table-statement, drop-table-statement}

class ParseTreeNode {
private:
  enum NODE_TYPE type;
  string value;
  vector<ParseTreeNode *> children;
public:
  ParseTreeNode(enum NODE_TYPE t) {
    type = t;
  }
};