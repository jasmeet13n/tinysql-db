#include <iostream>
#include <vector>
#include <gtest/gtest.h>

#include "parser.cc"
#include "parse_tree.cc"
#include "utils.cc"

TEST(UtilsTest, AttributeTypeList) {
  std::string test = "CREATE    TABLE test( id INT,    name STR20)";
  ParseTreeNode* ans = Parser::parseQuery(test);
  std::vector<std::string> cols;
  std::vector<enum FIELD_TYPE> dt;
  Utils::getAttributeTypeList(ans, cols, dt);
  EXPECT_EQ("id", cols[0]);
  EXPECT_EQ("name", cols[1]);
  EXPECT_EQ(INT, dt[0]);
  EXPECT_EQ(STR20, dt[1]);
}

TEST(UtilsTest, relationName) {
  std::string test = "CREATE    TABLE test( id INT,    name STR20)";
  ParseTreeNode* ans = Parser::parseQuery(test);
  std::string relationName = Utils::getTableName(ans);
  EXPECT_EQ("test", relationName);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
