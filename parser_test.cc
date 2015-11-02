#include <iostream>
#include <vector>

#include "parser.cc"
#include "parse_tree.cc"
#include "gtest/gtest.h"

using namespace std;

TEST(ParserTest, createTableTest) {
  string test = "CREATE    TABLE test( id INT,    name STR20)";

  ParseTreeNode* ans = Parser::parseQuery(test);
  
  
  //for (int i = 0; i < ans.size(); ++i) {
  //  cout << ans[i] << endl;
  //}
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
