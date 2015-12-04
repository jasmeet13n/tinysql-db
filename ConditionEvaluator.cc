#ifndef __CONDITION_EVALUATOR_INCLUDED
#define __CONDITION_EVALUATOR_INCLUDED

#include <unordered_map>
#include <utility>
#include <iostream>
#include <stack>
#include <string>

#include "./StorageManager/Block.h"
#include "./StorageManager/Config.h"
#include "./StorageManager/Disk.h"
#include "./StorageManager/Field.h"
#include "./StorageManager/MainMemory.h"
#include "./StorageManager/Relation.h"
#include "./StorageManager/Schema.h"
#include "./StorageManager/SchemaManager.h"
#include "./StorageManager/Tuple.h"
#include "utils.cc"
#include "parse_tree.cc"

class ConditionEvaluator {
private:
  class Factor {
  public:
    FIELD_TYPE type;
    Field field;
    int const_opr_var; // +1 for const, 0 for operator, -1 for variable
    int rel_offset;

    Factor(FIELD_TYPE t, int con, int int_val, std::string& str_val, int off) {
      type = t;
      const_opr_var = con;
      rel_offset = off;
      if (con == 1) {
        if (type == INT) {
          field.integer = int_val;
        } else {
          field.str = new std::string(str_val);
        }
      } else {
        field.str = new std::string(str_val);
      }
      //      if (con == false) {
      //        return;
      //      }
      //      if (t == INT) {
      //        field.integer = int_val;
      //      } else {
      //        field.str = new std::string(str_val);
      //      }
    }

    void printFactor() {
      std::string tf;
      if (const_opr_var == -1) {
        tf = "VARIABLE";
      } else if (const_opr_var == 0) {
        tf = "OPERATOR";
      } else {
        tf = "CONSTANT";
      }
      std::string str_or_int = "INT";
      if (type == STR20) {
        str_or_int = "STR20";
        std::cout << tf << " :: " << str_or_int << " :: " << (*(field.str)) << "\n";
      } else if (const_opr_var == -1){
        std::cout << tf << " :: " << str_or_int << " :: " << (*(field.str)) << "\n";
      } else {
        std::cout << tf << " :: " << str_or_int << " :: " << field.integer << "\n";
      }
    }
  };

  std::vector<Factor> postfix;
public:
  void initialize(ParseTreeNode* expression_tree_root, Relation* rel) {
    const string& table_name = rel->getRelationName();
    const Schema& schema = rel->getSchema();

    std::unordered_map<std::string, std::pair<int, FIELD_TYPE> > field_map;
    std::vector<std::string> fields = schema.getFieldNames();

    // Making a map of field names to field types and offset in tuple
    for (int i = 0; i < fields.size(); ++i) {
      FIELD_TYPE field_type = schema.getFieldType(i);
      // std::cout << (field_type == INT ? "INT" : "STR20") << "\n";
      field_map[fields[i]] = std::make_pair(i, field_type);
      // field_map[table_name + "." + fields[i]] = std::make_pair(i, field_type);
    }

    // Creating a postfix vector with Factor class.
    std::string empty_string = "";
    for (int i = 0; i < (expression_tree_root->children).size(); ++i) {
      string& val = expression_tree_root->children[i]->value;
      if (field_map.find(val) != field_map.end()) {
        std::pair<int, FIELD_TYPE>& p = field_map[val];
        //expression_tree_root->children[i]->type = NODE_TYPE::POSTFIX_VARIABLE;
        Factor f(p.second, -1, -1, val, p.first);
        postfix.push_back(f);
      } else {
        int con = 1;
        if (Parser::isOperator(val)) {
          con = 0;
        }

        Factor f(STR20, con, -1, val, -1);
        postfix.push_back(f);
      }
    }

    //    for (int i = 0; i < postfix.size(); ++i) {
    //      postfix[i].printFactor();
    //    }

    // Dry evaluation using stack to convert STR20 to INT where required
    std::stack<Factor*> st;
    std::string tmp_var = "TMP VAR";
    Factor bool_out(INT, -1, 1000, tmp_var, -1);
    for (int i = 0; i < postfix.size(); ++i) {
      if (postfix[i].const_opr_var == -1 || postfix[i].const_opr_var == 1) {
        st.push(&postfix[i]);
      } else {
        if (*(postfix[i].field.str) == "NOT") {
          continue;
        }

        Factor* f1 = st.top();
        st.pop();
        Factor* f2 = st.top();
        st.pop();

        if (f1->const_opr_var == 1) {
          if (f2->const_opr_var == -1) {
            if (f2->type == INT) {
              int converted_val = stoi(*(f1->field.str));
              f1->type = INT;
              delete f1->field.str;
              f1->field.integer = converted_val;
            }
          }
        } else if (f2->const_opr_var == 1) {
          if (f1->const_opr_var == -1) {
            if (f1->type == INT) {
              int converted_val = stoi(*(f2->field.str));
              f2->type = INT;
              delete f2->field.str;
              f2->field.integer = converted_val;
            }
          }
        }
        st.push(&bool_out);
      }
    }

    //    for (int i = 0; i < postfix.size(); ++i) {
    //      postfix[i].printFactor();
    //    }
  }

  bool evaluate(Tuple& tup) {
    std::stack<std::pair<FIELD_TYPE, Field> > st;

    Field f_true, f_false;
    f_true.integer = 1;
    f_false.integer = 0;
    std::pair<FIELD_TYPE, Field> bool_true = std::make_pair(INT, f_true);
    std::pair<FIELD_TYPE, Field> bool_false = std::make_pair(INT, f_false);

    for (int i = 0; i < postfix.size(); ++i) {
      if (postfix[i].const_opr_var == 1) {
        st.push(std::make_pair(postfix[i].type, postfix[i].field));
      } else if (postfix[i].const_opr_var == -1) {
        st.push(std::make_pair(postfix[i].type, tup.getField(postfix[i].rel_offset)));
      } else {
        std::string& opr = *(postfix[i].field.str);
        if (opr == "NOT") {
          std::pair<FIELD_TYPE, Field> f = st.top();
          st.pop();
          if (f.second.integer == 0) {
            f.second.integer = 1;
          } else {
            f.second.integer = 0;
          }
          st.push(f);
          continue;
        }
        std::pair<FIELD_TYPE, Field> f1 = st.top();
        st.pop();
        std::pair<FIELD_TYPE, Field> f2 = st.top();
        st.pop();

        Field f;
        if (opr == "+") {
          if (f1.first == INT) {
            f.integer = f1.second.integer + f2.second.integer;
            st.push(std::make_pair(INT, f));
          }
        } else if (opr == "-") {
          if (f1.first == INT) {
            f.integer = f2.second.integer - f1.second.integer;
            st.push(std::make_pair(INT, f));
          }
        } else if (opr == "*") {
          if (f1.first == INT) {
            f.integer = (f1.second.integer)*(f2.second.integer);
            st.push(std::make_pair(INT, f));
          }
        } else if (opr == "/") {
          if (f1.first == INT) {
            f.integer = (f2.second.integer)/(f1.second.integer);
            st.push(std::make_pair(INT, f));
          }
        } else if (opr == "=") {
          if (f1.first == INT) {
            if (f1.second.integer == f2.second.integer) {
              st.push(bool_true);
            } else {
              st.push(bool_false);
            }
          } else {
            if (*(f1.second.str) == *(f2.second.str)) {
              st.push(bool_true);
            } else {
              st.push(bool_false);
            }
          }
        } else if (opr == ">") {
          if (f1.first == INT) {
            if (f2.second.integer > f1.second.integer) {
              st.push(bool_true);
            } else {
              st.push(bool_false);
            }
          }
        } else if (opr == "<") {
          if (f1.first == INT) {
            if (f2.second.integer < f1.second.integer) {
              st.push(bool_true);
            } else {
              st.push(bool_false);
            }
          }
        } else if (opr == "AND") {
          if (f1.second.integer == 1 && f2.second.integer == 1) {
            st.push(bool_true);
          } else {
            st.push(bool_false);
          }
        } else if (opr == "OR") {
          if (f1.second.integer == 1 || f2.second.integer == 1) {
            st.push(bool_true);
          } else {
            st.push(bool_false);
          }
        }
      }
    }
    std::pair<FIELD_TYPE, Field> last = st.top();
    if (last.second.integer == 1) {
      return true;
    }
    return false;
  }
};

#endif
