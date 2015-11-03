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
#include "parse_tree.cc"
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
    string relation_name = getRelationName(root);
    vector<string> field_names = getColumnNames(root);
    vector<enum FIELD_TYPE> getDataTypes(root);
    //Create schema and Create Relation
    Schema schema(field_names,field_types);
    return schema_manager.createRelation(relation_name,schema);
  }

  bool dropTable(ParseTreeNode* root) {
    string relation_name = getRelationName(root);
    return schema_manager.deleteRelation(relation_name);
  }

  bool processQuery(std::string& query) {
    ParseTreeNode* root = Parser::parseQuery(query);
    if (root == nullptr) {
      return false;
    }

    if (root->type == NODE_TYPE::CREATE_TABLE_STATEMENT) {
      //
    } else if (root->type == NODE_TYPE::DROP_TABLE_STATEMENT) {
      //
    }
    return false;
  }
};

#endif
