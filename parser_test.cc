#include <iostream>
#include <vector>
#include <gtest/gtest.h>

#include "parser.cc"
#include "parse_tree.cc"
#include "utils.cc"

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
  //ASSERT_TRUE("SRT" == "SRT");
  
  //for (int i = 0; i < ans.size(); ++i) {
  //  cout << ans[i] << endl;
  //}
}

TEST(ParserTest, dropTableTest) {
  std::string test = "DROP    TABLE test";
  ParseTreeNode* ans = Parser::parseQuery(test);
  EXPECT_NE(nullptr, ans);

  ans = Parser::parseQuery("drop table test");
  EXPECT_NE(nullptr, ans);

  ans = Parser::parseQuery("Drop TAble test");
  EXPECT_NE(nullptr, ans);

  ans = Parser::parseQuery("Drop test");
  EXPECT_EQ(nullptr, ans);

  ans = Parser::parseQuery("drops TAble test");
  EXPECT_EQ(nullptr, ans);

  //for (int i = 0; i < ans.size(); ++i) {
  //  cout << ans[i] << endl;
  //}
}

TEST(ParserTest, insertIntoTest) {
  std::string test = "INSERT    INTO test (id, value, name, text) VALUES ";
  ParseTreeNode* ans = Parser::parseQuery(test);
  EXPECT_NE(nullptr, ans);

  ans = Parser::parseQuery("insert into test (id, value, name, text) VALUES ");
  EXPECT_NE(nullptr, ans);

  ans = Parser::parseQuery("Insert Into test (id, value, name, text) VALUES ");
  EXPECT_NE(nullptr, ans);

  ans = Parser::parseQuery("INsert test (id, value, name, text) VALUES ");
  EXPECT_EQ(nullptr, ans);

  ans = Parser::parseQuery("Inseert TAble test (id, value, name, text) VALUES ");
  EXPECT_EQ(nullptr, ans);

  //for (int i = 0; i < ans.size(); ++i) {
  //  cout << ans[i] << endl;
  //}
}

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
