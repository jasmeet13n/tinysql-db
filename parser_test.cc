#include <iostream>
#include <vector>
#include <gtest/gtest.h>

#include "parser.cc"
#include "parse_tree.cc"

TEST(ParserTest, createTableTest) {
  std::string test = "CREATE    TABLE test( id INT,    name STR20)";
  ParseTreeNode* ans = Parser::parseQuery(test);
  EXPECT_NE(nullptr, ans);

  ans = Parser::parseQuery("create table test (id INT, name STR20)");
  EXPECT_NE(nullptr, ans);

  ans = Parser::parseQuery("Create TAble test (id INT, name STR20)");
  EXPECT_NE(nullptr, ans);

  ans = Parser::parseQuery("Create test (id INT, name STR20)");
  EXPECT_EQ(nullptr, ans);

  ans = Parser::parseQuery("Cresate TAble test (id INT, name STR20)");
  EXPECT_EQ(nullptr, ans);
  
  //for (int i = 0; i < ans.size(); ++i) {
  //  cout << ans[i] << endl;
  //}
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
