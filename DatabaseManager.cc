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
#include "MemoryManager.cc"

class DatabaseManager {
private:
	MainMemory* mem;
	Disk* disk;
	SchemaManager schema_manager;
	MemoryManager mManager;

	bool appendTupleToRelation(Relation* relation_ptr, int memory_block_index, Tuple& tuple) {
	  Block* block_ptr;
	  if (relation_ptr->getNumOfBlocks() == 0) {
	    cout << "The relation is empty" << endl;
	    cout << "Get the handle to the memory block " << memory_block_index << " and clear it" << endl;
	    block_ptr = mem->getBlock(memory_block_index);
	    block_ptr->clear(); //clear the block
	    block_ptr->appendTuple(tuple); // append the tuple
	    cout << "Write to the first block of the relation" << endl;
	    relation_ptr->setBlock(relation_ptr->getNumOfBlocks(), memory_block_index);
	  } else {
	    cout << "Read the last block of the relation into memory block: " << memory_block_index << endl;
	    relation_ptr->getBlock(relation_ptr->getNumOfBlocks() - 1, memory_block_index);
	    block_ptr = mem->getBlock(memory_block_index);

	    if (block_ptr->isFull()) {
	      cout << "(The block is full: Clear the memory block and append the tuple)" << endl;
	      block_ptr->clear(); //clear the block
	      block_ptr->appendTuple(tuple); // append the tuple
	      cout << "Write to a new block at the end of the relation" << endl;
	      relation_ptr->setBlock(relation_ptr->getNumOfBlocks(), memory_block_index); //write back to the relation
	    } else {
	      cout << "(The block is not full: Append it directly)" << endl;
	      block_ptr->appendTuple(tuple); // append the tuple
	      cout << "Write to the last block of the relation" << endl;
	      relation_ptr->setBlock(relation_ptr->getNumOfBlocks()-1,memory_block_index); //write back to the relation
	    }
	  }
	  return true;
	}

public:
	DatabaseManager(MainMemory* m, Disk* d) : schema_manager(m, d), mManager(m) {
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

    // for (int i = 0; i < att_list.size(); ++i) {
    //  std::cout << att_list[i] << " :: " << value_list[i] << std::endl;
    // }

    // get schema from relation
    Schema s = r->getSchema();
    Tuple t = r->createTuple();

    // get field types from schema
    for (int i = 0; i < att_list.size(); ++i) {
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

    int free_block_index = mManager.getFreeBlockIndex();
    if (free_block_index == -1) {
      return false;
    }
    bool result = appendTupleToRelation(r, free_block_index, t);
    mManager.releaseBlock(free_block_index);

    std::cout << *mem << std::endl;
    std::cout << *r << std::endl;

    if (result == false) {
      return false;
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
