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

  bool insertTuplesIntoTable(std::string table_name, std::vector<Tuple> tuples) {
    Relation* r = schema_manager.getRelation(table_name);
    bool result = true;
    int free_block_index = mManager.getFreeBlockIndex();
    if (free_block_index == -1) {
      return false;
    }
    for(int i = 0; i < tuples.size(); i++) {
      result = appendTupleToRelation(r, free_block_index, tuples[i]);
      if(!result) {
        return false;
        mManager.releaseBlock(free_block_index);
      }
    }
    mManager.releaseBlock(free_block_index);
    return true;
  }

  bool processInsertStatement(ParseTreeNode* root) {
    std::string table_name = Utils::getTableName(root);
    Relation* r = schema_manager.getRelation(table_name);
    if (r == nullptr) {
      return false;
    }

    std::vector<Tuple> insert_tuples;

    if(root->children[4]->children[0]->type == NODE_TYPE::VALUES_LITERAL) {
      // get attribute_list
      std::vector<std::string> att_list;
      Utils::getAttributeList(root, att_list);

      // get value_list
      std::vector<std::string> value_list;
      Utils::getValueList(root, value_list);

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
      insert_tuples.push_back(t);
    }
    else {
      processSelectSingleTable(root->children[4]->children[0], insert_tuples);
    }

    return insertTuplesIntoTable(table_name, insert_tuples);
  }

  bool processSelectStatement(ParseTreeNode* root) {
    int index = root->children[1]->type == NODE_TYPE::DISTINCT_LITERAL ? 4 : 3;
    if(root->children[index]->children.size() == 1) {
      std::vector<Tuple> tuples;
      return processSelectSingleTable(root, tuples, true);
    }
    else {
      // make logical query plan
      LQueryTreeNode* lqt_root = Utils::createLogicalQueryTree(root);
    }
    return false;
  }

  bool processSelectSingleTable(ParseTreeNode* root, std::vector<Tuple>& insert_tuples, bool print=false) {
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

    if(print) {
      for(int i = 0; i < field_names.size(); i++) {
        std::cout << field_names[i] << "\t";
      }
      std::cout << std::endl;
    }

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
          if(print) 
            std::cout << tuples[j] << std::endl;
          else
            insert_tuples.push_back(tuples[j]);
        } else {
          Tuple temp_tuple = r->createTuple();
          for (int k = 0; k < field_indices.size(); ++k) {
            if (field_types[k] == INT) {
              if(print) 
                std::cout << tuples[j].getField(field_indices[k]).integer << "\t";
              else
                temp_tuple.setField(k, tuples[j].getField(field_indices[k]).integer);
            } else {
              if(print)
                std::cout << *(tuples[j].getField(field_indices[k]).str) << "\t";
              else
                temp_tuple.setField(k, *(tuples[j].getField(field_indices[k]).str));
            }
          }
          if(print)
            std::cout << std::endl;
          else
            insert_tuples.push_back(temp_tuple);
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
    int index = root->children[1]->type == NODE_TYPE::DISTINCT_LITERAL ? 6 : 5;
    if(root->children.size() > 5 && root->children[index]->type == NODE_TYPE::POSTFIX_EXPRESSION) {
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
      ParseTreeNode* exp_root = (root->children[1]->type == NODE_TYPE::DISTINCT_LITERAL) ? root->children[6] : root->children[5];
      for(int i = 0; i < exp_root->children.size(); i++) {
        ParseTreeNode* temp = exp_root->children[i];
        if(temp->type == NODE_TYPE::POSTFIX_OPERAND && temp->value.find(".") == std::string::npos) {
          temp->value = column_names_map[temp->value];  
        }
      }
    }
  }

  // Cross Join - One Pass Algorithm
  Relation* crossJoinOnePass(Relation* small, Relation* large) {
    int small_n = small->getNumOfBlocks();
    int large_n = large->getNumOfBlocks();
    std::string small_relation_name = small->getRelationName();
    std::string large_relation_name = large->getRelationName();
    Schema small_schema = schema_manager.getSchema(small_relation_name);
    Schema large_schema = schema_manager.getSchema(large_relation_name);
    std::vector<std::string> small_column_names = small_schema.getFieldNames();
    std::vector<enum FIELD_TYPE> small_data_types = small_schema.getFieldTypes();
    std::vector<std::string> large_column_names = large_schema.getFieldNames();
    std::vector<enum FIELD_TYPE> large_data_types = large_schema.getFieldTypes();

    // add table name to each column name
    for(int  i = 0; i < small_column_names.size(); i++)
      small_column_names[i] = small_relation_name + "." + small_column_names[i];

    for(int  i = 0; i < large_column_names.size(); i++)
      large_column_names[i] = large_relation_name + "." + large_column_names[i];

    std::vector<std::string> new_column_names;
    std::vector<enum FIELD_TYPE> new_data_types;

    // Join column_names and data_types
    new_column_names.insert(new_column_names.end(), small_column_names.begin(), small_column_names.end());
    new_column_names.insert(new_column_names.end(), large_column_names.begin(), large_column_names.end());
    new_data_types.insert(new_data_types.end(), small_data_types.begin(), small_data_types.end());
    new_data_types.insert(new_data_types.end(), large_data_types.begin(), large_data_types.end());

    Schema new_schema(new_column_names, new_data_types);
    std::string new_relation_name = small_relation_name + large_relation_name;
    Relation* new_relation = schema_manager.createRelation(new_relation_name, new_schema);

    // Get small_n free blocks from main memory
    std::vector<int> small_block_indices;
    if(!mManager.getNFreeBlockIndices(small_block_indices, small_n)) {
      return nullptr;
    }

    // Copy blocks from small relation to free_blocks in memory
    for(int i = 0; i < small_n; i++) {
      small->getBlock(i, small_block_indices[i]);
    }

    int large_mem_block_index = mManager.getFreeBlockIndex();
    if(large_mem_block_index == -1)
      return nullptr;

    // Join operation
    // for large_n blocks
    for(int i = 0; i < large_n; i++) { 
      large->getBlock(i, large_mem_block_index);
      Block* large_mem_block = mem->getBlock(large_mem_block_index);
      std::vector<Tuple> large_tuples = large_mem_block->getTuples();
      // for each tuple in block
      for(int t1 = 0; t1 < large_tuples.size(); t1++) { 
        if(!large_tuples[t1].isNull()) {
          // for small_n blocks in memory
          for(int j = 0; j < small_n; j++) { 
            Block* small_mem_block = mem->getBlock(small_block_indices[j]);
            std::vector<Tuple> small_tuples = small_mem_block->getTuples();
            // for each tuple in block
            for(int t2 = 0; t2 < small_tuples.size(); t2++) { 
              if(!small_tuples[t2].isNull()) {
                Tuple new_tuple = new_relation->createTuple();

                for(int index = 0; index < small_column_names.size(); index++) {
                  FIELD_TYPE f = new_schema.getFieldType(small_column_names[index]);
                  if(f == INT) {
                    new_tuple.setField(index, small_tuples[t2].getField(index).integer);
                  }
                  else {
                    new_tuple.setField(index, *(small_tuples[t2].getField(index).str));
                  }
                }

                for(int index = 0; index < large_column_names.size(); index++) {
                  FIELD_TYPE f = new_schema.getFieldType(large_column_names[index]);
                  if(f == INT) {
                    new_tuple.setField(small_column_names.size()+index, 
                      large_tuples[t1].getField(index).integer);
                  }
                  else {
                    new_tuple.setField(small_column_names.size()+index, 
                      *(large_tuples[t1].getField(index).str));
                  }
                }

                int free_block_index = mManager.getFreeBlockIndex();
                if(free_block_index == -1)
                  return nullptr;
                appendTupleToRelation(new_relation, free_block_index, new_tuple);
                mManager.releaseBlock(free_block_index);
              }
            }
          }
        }
      }
    }

    mManager.releaseBlock(large_mem_block_index);
    mManager.releaseNBlocks(small_block_indices);
    return new_relation;
  }

<<<<<<< HEAD
  // Assume no holes in mem_blocks [Buble Sort]
  void sortTuples(std::vector<int> mem_block_indices, string column_name, string relation_name) {
    int blocks = mem_block_indices.size();
    int tuples_per_block = (mem->getBlock(mem_block_indices[0]))->getNumTuples(); //max tuples per block
    int j = 0;
    int n = (blocks - 1) * tuples_per_block;
    n = n + (mem->getBlock(mem_block_indices[mem_block_indices.size() - 1]))->getNumTuples();
    bool swapped = true;
    Relation* rel_ptr = schema_manager.getRelation(relation_name);
    Schema s = rel_ptr->getSchema();

    while(swapped) {
      swapped = false;
      j++;
      for(int i =  0; i < n - 1 - j; i++) {
        int block_num_1 = i / tuples_per_block;
        int block_num_2 = (i + 1) / tuples_per_block;
        Block* block1 = mem->getBlock(block_num_1);
        Block* block2 = mem->getBlock(block_num_2);
        int tuple1_offset = i % tuples_per_block;
        int tuple2_offset = (i + 1) % tuples_per_block;
        Field field1 = (block1->getTuple(tuple2_offset)).getField(column_name);
        Field field2 = (block2->getTuple(tuple2_offset)).getField(column_name);
        if(compareFields(s.getFieldType(column_name), field1, field2) == 1) {
          Tuple temp = (block1->getTuple(tuple1_offset)).getField(column_name);
          block1->setTuple(tuple1_offset, block2->getTuple(tuple2_offset));
          block2->setTuple(tuple2_offset, temp);
          swapped = true;
        }
      }
    }
  }

  int compareFields(enum FIELD_TYPE f, union Field& field1, union Field& field2) {
    if(f == INT) {
      if(field1.integer > field2.integer)
        return 1;
      else if(field1.integer < field2.integer)
        return -1;
      return 0;
    }
    if(*field1.str > *field2.str)
      return 1;
    else if(*field1.str < *field2.str)
      return -1;
    return 0;
  }

  Relation* crossJoinWithConditionOnePass(std::string& rSmall, std::string& rLarge,
      ParseTreeNode* postFixExpr, std::vector<std::string>& projList, bool storeOutput) {

    return nullptr;
  }

  Relation* crossJoinWithConditionTwoPass(std::string& r1, std::string& r2,
      ParseTreeNode* postFixExpr, std::vector<std::string>& projList, bool storeOutput) {

    return nullptr;
  }

  Relation* crossJoinWithCondition(std::string& r1, std::string& r2,
      ParseTreeNode* postFixExpr, std::vector<std::string>& projList, bool storeOutput = false) {
    // Check if one of the relation can fit in memory
    if (true) {
      return crossJoinWithConditionOnePass(r1, r2, postFixExpr, projList, storeOutput);
    } else {
      return crossJoinWithConditionTwoPass(r1, r2, postFixExpr, projList, storeOutput);
    }
    return nullptr;
  }

  bool processQuery(std::string& query) {
    disk->resetDiskIOs();
    disk->resetDiskTimer();

    bool result = false;

    std::cout << "Q>"<< query << std::endl;
    ParseTreeNode* root = Parser::parseQuery(query);
    if (root == nullptr) {
      return false;
    }

    if (root->type == NODE_TYPE::CREATE_TABLE_STATEMENT) {
      result = processCreateTableStatement(root);
    } else if (root->type == NODE_TYPE::DROP_TABLE_STATEMENT) {
      result = processDropTableStatement(root);
    } else if (root->type == NODE_TYPE::INSERT_STATEMENT) {
      result = processInsertStatement(root);
    } else if (root->type == NODE_TYPE::SELECT_STATEMENT) {
      result = processSelectStatement(root);
    } else if (root->type == NODE_TYPE::DELETE_STATEMENT) {
      result = processDeleteStatement(root);
    }

    std::cout << "Disk I/O: " << disk->getDiskIOs() << std::endl;
    std::cout << "Execution Time: " << disk->getDiskTimer() << " ms" << std::endl;
    return result;
  }
};

#endif
