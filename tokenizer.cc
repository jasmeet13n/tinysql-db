#include <string>
#include <vector>

using namespace std;

class Tokenizer {
public:
  static vector<string> getTokens(const string& text) {
    vector<string> ans;
    int last = 0;
    int i;
    for (i = 0; i < text.size(); ++i) {
      if (text[i] == ',' || text[i] == '(' || text[i] == ')' || text[i] == ' ') {
        if (last < i) {
          ans.push_back(text.substr(last, i - last));
        }
        last = i + 1;
        if (text[i] != ' ') {
          ans.push_back(string(1, text[i]));
        }
      }
    }
    if (last < i) {
      ans.push_back(text.substr(last, i - last));
    }
    return ans;
  }
};