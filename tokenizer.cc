#ifndef __TOKENIZER_INCLUDED
#define __TOKENIZER_INCLUDED

#include <string>
#include <vector>

class Tokenizer {
public:
  static std::vector<std::string> getTokens(const std::string& text) {
    std::vector<std::string> ans;
    int last = 0;
    int i;
    for (i = 0; i < text.size(); ++i) {
      if (text[i] == ',' || text[i] == '(' || text[i] == ')' || text[i] == ' ') {
        if (last < i) {
          ans.push_back(text.substr(last, i - last));
        }
        last = i + 1;
        if (text[i] != ' ') {
          ans.push_back(std::string(1, text[i]));
        }
      }
    }
    if (last < i) {
      ans.push_back(text.substr(last, i - last));
    }
    return ans;
  }
};

#endif
