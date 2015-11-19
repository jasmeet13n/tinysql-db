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
#include "ConditionEvaluator.cc"

class DatabaseManager {
private:
	MainMemory* mem;
	Disk* disk;
	SchemaManager schema_manager;
	MemoryManager mManager;

	bool appendTupleToRelation(Relation* relation_ptr, int memory_block_index, Tuple& tuple) {
	  Block* block_ptr;
	  if (relation_ptr->getNumOfBlocks() == 0) {
	    //cout << "The relation is empty" << endl;
	    //cout << "Get the handle to the memory block " << memory_block_index << " and clear it" << endl;
	    block_ptr = mem->getBlock(memory_block_index);
	    block_ptr->clear(); //clear the block
	    block_ptr->appendTuple(tuple); // append the tuple
	    //cout << "Write to the first block of the relation" << endl;
	    relation_ptr->setBlock(relation_ptr->getNumOfBlocks(), memory_block_index);
	  } else {
	    //cout << "Read the last block of the relation into memory block: " << memory_block_index << endl;
	    relation_ptr->getBlock(relation_ptr->getNumOfBlocks() - 1, memory_block_index);
	    block_ptr = mem->getBlock(memory_block_index);

	    if (block_ptr->isFull()) {
	      //cout << "(The block is full: Clear the memory block and append the tuple)" << endl;
	      block_ptr->clear(); //clear the block
	      block_ptr->appendTuple(tuple); // append the tuple
	      //cout << "Write to a new block at the end of the relation" << endl;
	      relation_ptr->setBlock(relation_ptr->getNumOfBlocks(), memory_block_index); //write back to the relation
	    } else {
	      //cout << "(The block is not full: Append it directly)" << endl;
	      block_ptr->appendTuple(tuple); // append the tuple
	      //cout << "Write to the last block of the relation" << endl;
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

    //std::cout << *mem << std::endl;
    //std::cout << *r << std::endl;

    if (result == false) {
      return false;
    }
    return true;
  }

  bool processSelectStatement(ParseTreeNode* root) {
    //Only for single table in table-list
    std::string table_name = root->children[1]->type == NODE_TYPE::DISTINCT_LITERAL\
     ? root->children[4]->children[0]->value : root->children[3]->children[0]->value;
    Relation* r = schema_manager.getRelation(table_name);
    if(r == nullptr)
      return false;

    // Get a memory block
    int free_block_index = mManager.getFreeBlockIndex();
    if (free_block_index == -1) {
      return false;
    }

    std::vector<std::string> field_names;
    std::vector<enum FIELD_TYPE> field_types;
    std::vector<int> field_indices;

    Utils::getSelectList(root, field_names);
    const Schema& s = schema_manager.getSchema(table_name);

    bool isSelectStar = false;
    if (field_names[0] == "*") {
      isSelectStar = true;
    }

    if(isSelectStar) {
      field_names = s.getFieldNames();
    } else {
      for (int i = 0; i < field_names.size(); ++i) {
        int index = s.getFieldOffset(field_names[i]);
        field_indices.push_back(index);
        field_types.push_back(s.getFieldType(index));
      }
    }

    for(int i = 0; i < field_names.size(); i++) {
      std::cout << field_names[i] << "\t";
    }
    std::cout << std::endl;

    ConditionEvaluator eval;
    if (root->children.size() > 5)
      eval.initialize(root->children[5], r);
  
    for(int i = 0; i < r->getNumOfBlocks(); i++) {
      r->getBlock(i, free_block_index);
      Block* block_ptr = mem->getBlock(free_block_index);
      std::vector<Tuple> tuples = block_ptr->getTuples();

      for (int j = 0; j < tuples.size(); ++j) {
        if (root->children.size() > 5) {
          bool ans = eval.evaluate(tuples[j]);
          if (ans == false) {
            continue;
          }
          //std::cout << (ans == true? "TRUE" : "FALSE") << std::endl;
        }

        if (isSelectStar) {
          std::cout << tuples[j] << std::endl;
        } else {
          for (int k = 0; k < field_indices.size(); ++k) {
            if (field_types[k] == INT) {
              std::cout << tuples[j].getField(field_indices[k]).integer << "\t";
            } else {
              std::cout << *(tuples[j].getField(field_indices[k]).str) << "\t";
            }
          }
          std::cout << std::endl;
        }
      }
    }
    mManager.releaseBlock(free_block_index);
    return true;
  }

  bool processDeleteStatement(ParseTreeNode* root) {
    return false;
  }

  void changeAttributeNames(ParseTreeNode* root) {
    if(root->children.size() > 5 && root->children[5]->type == NODE_TYPE::POSTFIX_EXPRESSION) {
      std::vector<std::string> tables;
      Utils::getTableList(root, tables);
      std::unordered_map<std::string, std::string> column_names_map;
      for(int i = 0; i < tables.size(); i++) {
        Schema schema = schema_manager.getSchema(tables[i]);
        std::vector<std::string> field_names = schema.getFieldNames();
        for(int j = 0; j < field_names.size(); j++) {
          column_names_map[field_names[j]] = tables[i] + "." + field_names[j];
        }
      }
      ParseTreeNode* exp_root = root->children[5];
      for(int i = 0; i < exp_root->children.size(); i++) {
        ParseTreeNode* temp = exp_root->children[i];
        if(temp->type == NODE_TYPE::POSTFIX_OPERAND && temp->value.find(".") == std::string::npos) {
          temp->value = column_names_map[temp->value];  
        }
      }
    }
  }

  bool processQuery(std::string& query) {
    std::cout << "Q>"<< query << std::endl;
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
    } else if (root->type == NODE_TYPE::SELECT_STATEMENT) {
      return processSelectStatement(root);
    } else if (root->type == NODE_TYPE::DELETE_STATEMENT) {
      return processDeleteStatement(root);
    }
    return false;
  }
};

#endif
