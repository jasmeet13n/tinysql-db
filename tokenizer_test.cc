#include <iostream>
#include <vector>
#include <gtest/gtest.h>
#include "tokenizer.cc"

using namespace std;

TEST(TokenizerTest, createTableTest) {
  string test = "CREATE    TABLE test( id INT,    name STR20)";
  vector<string> ans;

  ans = Tokenizer::getTokens(test);
  EXPECT_EQ(10, ans.size());

  EXPECT_EQ("CREATE", ans[0]);
  EXPECT_EQ("TABLE", ans[1]);
  EXPECT_EQ("test", ans[2]);
  EXPECT_EQ("(", ans[3]);
  EXPECT_EQ("id", ans[4]);
  EXPECT_EQ("INT", ans[5]);
  EXPECT_EQ(",", ans[6]);
  EXPECT_EQ("name", ans[7]);
  EXPECT_EQ("STR20", ans[8]);
  EXPECT_EQ(")", ans[9]);

  //for (int i = 0; i < ans.size(); ++i) {
  //  cout << ans[i] << endl;
  //}
}

TEST(TokenizerTest, insertIntoTest) {
  string test = "INSERT INTO test (id, text) VALUES (\"20\", \"some random text\")";
  vector<string> ans;

  ans = Tokenizer::getTokens(test);
  EXPECT_EQ(14, ans.size());

  EXPECT_EQ("INSERT", ans[0]);
  EXPECT_EQ("INTO", ans[1]);
  EXPECT_EQ("test", ans[2]);
  EXPECT_EQ("(", ans[3]);
  EXPECT_EQ("id", ans[4]);
  EXPECT_EQ(",", ans[5]);
  EXPECT_EQ("text", ans[6]);
  EXPECT_EQ(")", ans[7]);
  EXPECT_EQ("VALUES", ans[8]);
  EXPECT_EQ("(", ans[9]);
  EXPECT_EQ("20", ans[10]);
  EXPECT_EQ(",", ans[11]);
  EXPECT_EQ("some random text", ans[12]);
  EXPECT_EQ(")", ans[13]);

  //cout << test << endl;
  //for (int i = 0; i < ans.size(); ++i) {
  //  cout << ans[i] << endl;
  //}
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
