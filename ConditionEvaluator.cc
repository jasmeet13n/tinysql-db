#ifndef __CONDITION_EVALUATOR_INCLUDED
#define __CONDITION_EVALUATOR_INCLUDED

#include <unordered_map>
#include <utility>

#include "./StorageManager/Field.h"
#include "./StorageManager/Relation.h"
#include "./StorageManager/Schema.h"
#include "utils.cc"
#include "parse_tree.cc"

class ConditionEvaluator {
private:
  class Factor {
  public:
    FIELD_TYPE type;
    Field field;
    bool is_const;
    int rel_offset;
    Factor(FIELD_TYPE t, bool con, int int_val, std::string& str_val, int off) {
      type = t;
      is_const = con;
      rel_offset = off;
      if (con == false) {
        return;
      }
      if (t == INT) {
        field.integer = int_val;
      } else {
        field.str = new std::string(str_val);
      }
    }
  };

  std::vector<Factor> postfix;
public:
  ConditionEvaluator(ParseTreeNode* expression_tree_root, Relation* rel) {
    const string& table_name = rel->getRelationName();
    const Schema& schema = rel->getSchema();

    std::unordered_map<std::string, std::pair<int, FIELD_TYPE> > field_map;
    std::vector<std::string> fields = schema.getFieldNames();

    for (int i = 0; i < fields.size(); ++i) {
      int field_type = schema.getFieldType(i);
      field_map[fields[i]] = std::make_pair(i, field_type);
      // field_map[table_name + "." + fields[i]] = std::make_pair(i, field_type);
    }


    for (int i = 0; i < (expression_tree_root->children).size(); ++i) {
      string& val = expression_tree_root->children[i]->value;
      if (field_map.find(val) != field_map.end()) {
        std::pair& p = field_map[val];
        Factor f(p.second, false, -1, "", p.first);
        postfix.push_back(f);
      } else {

      }
    }
  }
};

#endif
