#ifndef __DB_MANAGER_INCLUDED
#define __DB_MANAGER_INCLUDED

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
#include "parser.cc"
#include "parse_tree.cc"

class DatabaseManager {
private:
	MainMemory* mem;
	Disk* disk;
	SchemaManager schema_manager;

public:
	DatabaseManager(MainMemory* m, Disk* d) : schema_manager(m, d) {
    this->mem = m;
    this->disk = d;
  }

  Relation* createTable(ParseTreeNode* root) {
    //Extract Relation Name, Field Names and Field Types
    std::string table_name = Utils::getTableName(root);
    std::vector<std::string> column_names;
    std::vector<enum FIELD_TYPE> data_types;
    Utils::getAttributeTypeList(root, column_names, data_types);
    //Create schema and Create Relation
    Schema schema(column_names, data_types);
    return schema_manager.createRelation(table_name,schema);
  }

  bool processCreateTableStatement(ParseTreeNode* root) {
    Relation* newRelation = createTable(root);
    if (newRelation == nullptr) {
      return false;
    }
    return true;
  }

  bool processDropTableStatement(ParseTreeNode* root) {
    std::string table_name = Utils::getTableName(root);
    return schema_manager.deleteRelation(table_name);
  }

  bool processInsertStatement(ParseTreeNode* root) {
    std::string table_name = Utils::getTableName(root);
    Relation* r = schema_manager.getRelation(table_name);
    if (r == nullptr) {
      return false;
    }

    // get attribute_list
    std::vector<std::string> att_list;
    Utils::getAttributeList(root, att_list);

    // get value_list
    std::vector<std::string> value_list;
    Utils::getValueList(root, value_list);

    for (int i = 0; i < att_list.size(); ++i) {
      std::cout << att_list[i] << " :: " << value_list[i] << std::endl;
    }

    // get schema from relation
    Schema s = r->getSchema();

    // get field types from schema
    for (int i = 0; i < att_list.size(); ++i) {
      Tuple t = r->createTuple();
      FIELD_TYPE f = s.getFieldType(att_list[i]);
      bool result = true;
      if (f == INT) {
        int value = stoi(value_list[i], nullptr, 10);
        result = t.setField(att_list[i], value);
      } else {
        result = t.setField(att_list[i], value_list[i]);
      }
      if (result == false) {
        return false;
      }
    }
    return true;
  }

  bool processQuery(std::string& query) {
    ParseTreeNode* root = Parser::parseQuery(query);
    if (root == nullptr) {
      return false;
    }

    if (root->type == NODE_TYPE::CREATE_TABLE_STATEMENT) {
      return processCreateTableStatement(root);
    } else if (root->type == NODE_TYPE::DROP_TABLE_STATEMENT) {
      return processDropTableStatement(root);
    } else if (root->type == NODE_TYPE::INSERT_STATEMENT) {
      return processInsertStatement(root);
    }
    return false;
  }
};

#endif
