#include <string>
#include <vector>

enum NODE_TYPE {CREATE, DROP};

using namespace std;

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