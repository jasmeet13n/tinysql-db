#ifndef __DB_MANAGER_INCLUDED
#define __DB_MANAGER_INCLUDED

#include <string>
#include <algorithm>
#include <unordered_map>

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
  std::vector<std::string> temp_relations;

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

  bool appendTupleToMemBlock(int memory_block_index, Tuple& tuple) {
    Block* block_ptr;
    block_ptr = mem->getBlock(memory_block_index);
    if(block_ptr->isFull()) {
      return false;
    }
    else {
      block_ptr->appendTuple(tuple);
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
        mManager.releaseBlock(free_block_index);
        return false;
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

  bool processSelectSingleTable(ParseTreeNode* root, std::vector<Tuple>& insert_tuples, bool print=false) {
    //Only for single table in table-list
    int index = root->children[1]->type == NODE_TYPE::DISTINCT_LITERAL ? 4 : 3;
    std::string table_name = root->children[index]->children[0]->value;
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
    if (root->children.size() > 5 && root->children[index+1]->type == NODE_TYPE::WHERE_LITERAL)
      eval.initialize(root->children[index+2], r);

    for(int i = 0; i < r->getNumOfBlocks(); i++) {
      r->getBlock(i, free_block_index);
      Block* block_ptr = mem->getBlock(free_block_index);
      std::vector<Tuple> tuples = block_ptr->getTuples();

      for (int j = 0; j < tuples.size(); ++j) {
        if (root->children.size() > 5 && root->children[index+1]->type == NODE_TYPE::WHERE_LITERAL) {
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

  void sortOnePass(string relation_name, string column_name) {
    Relation* rel_ptr = schema_manager.getRelation(relation_name);
    int nBlocks = rel_ptr->getNumOfBlocks();
    std::vector<int> mem_block_indices;
    if(!mManager.getNFreeBlockIndices(mem_block_indices, nBlocks)) {
      return;
    }
    for(int i = 0; i < nBlocks; i++) {
      rel_ptr->getBlock(i, mem_block_indices[i]);
    }
    sortTuples(mem_block_indices, relation_name, column_name);
    printFieldNames(rel_ptr->getSchema());
    for(int i = 0; i < nBlocks; i++) {
      Block* block = mem->getBlock(mem_block_indices[i]);
      std::vector<Tuple> tuples = block->getTuples();
      for(int j = 0; j < tuples.size(); j++) {
        std::cout << tuples[j] << std::endl;
      }
    }
  }

  // Assume no holes in mem_blocks [Buble Sort]
  void sortTuples(std::vector<int> mem_block_indices, string relation_name, string column_name) {
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
          Tuple temp = block1->getTuple(tuple1_offset);
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

  Schema createCommonSchema(Relation* small, Relation* large, std::unordered_map<std::string, bool>& projListMap,
      std::unordered_map<int, int>& small_to_new, std::unordered_map<int, int>& large_to_new) {
    std::string small_relation_name = small->getRelationName();
    std::string large_relation_name = large->getRelationName();
    Schema small_schema = schema_manager.getSchema(small_relation_name);
    Schema large_schema = schema_manager.getSchema(large_relation_name);
    std::vector<std::string> small_column_names = small_schema.getFieldNames();
    std::vector<enum FIELD_TYPE> small_data_types = small_schema.getFieldTypes();
    std::vector<std::string> large_column_names = large_schema.getFieldNames();
    std::vector<enum FIELD_TYPE> large_data_types = large_schema.getFieldTypes();

    for(int  i = 0; i < small_column_names.size(); i++)
      small_column_names[i] = small_relation_name + "." + small_column_names[i];

    for(int  i = 0; i < large_column_names.size(); i++)
      large_column_names[i] = large_relation_name + "." + large_column_names[i];

    std::vector<std::string> new_column_names;
    std::vector<enum FIELD_TYPE> new_data_types;

    bool starPresent = false;
    if (projListMap.find("*") != projListMap.end())
      starPresent = true;


    for(int i = 0; i < small_column_names.size(); i++) {
      if(starPresent || projListMap.find(small_column_names[i]) != projListMap.end()) {
        new_column_names.push_back(small_column_names[i]);
        new_data_types.push_back(small_data_types[i]);
        small_to_new[i] = new_column_names.size() - 1;
      }
    }

    for(int i = 0; i < large_column_names.size(); i++) {
      if(starPresent || projListMap.find(large_column_names[i]) != projListMap.end()) {
        new_column_names.push_back(large_column_names[i]);
        new_data_types.push_back(large_data_types[i]);
        large_to_new[i] = new_column_names.size() - 1;
      }
    }

    Schema new_schema(new_column_names, new_data_types);
    return new_schema;
  }

  void printFieldNames(Schema schema) {
    std::vector<std::string> field_names;
    field_names = schema.getFieldNames();
    for(int i = 0; i < field_names.size(); i++) {
      std::cout << field_names[i] << "\t";
    }
    std::cout << std::endl;
  }

  Relation* crossJoinWithCondition(std::string rSmall, std::string rLarge,
      ParseTreeNode* postFixExpr, std::unordered_map<std::string, bool>& projListMap, bool storeOutput) {
    Relation* small = schema_manager.getRelation(rSmall);
    Relation* large = schema_manager.getRelation(rLarge);
    int small_n = small->getNumOfBlocks();
    int large_n = large->getNumOfBlocks();

    if (small_n > large_n) {
      std::swap(small_n, large_n);
      std::swap(small, large);
      std::swap(rSmall, rLarge);
    }

    //create new schema with projection list
    std::unordered_map<int, int> small_to_new;
    std::unordered_map<int, int> large_to_new;
    Schema new_schema = createCommonSchema(small, large, projListMap, small_to_new, large_to_new);

    //create new relation
    std::string rNew = rSmall + "_" + rLarge;
    Relation* new_relation = schema_manager.createRelation(rNew, new_schema);

    int large_mem_block_index = mManager.getFreeBlockIndex();
    if (large_mem_block_index == -1 ) {
      return nullptr;
    }

    int output_mem_block_index = -1;
    if (storeOutput) {
      output_mem_block_index = mManager.getFreeBlockIndex();
      if (output_mem_block_index == -1) {
        return nullptr;
      }
    }

    std::vector<int> small_mem_block_indices;
    if(!mManager.getAllFreeBlockIndices(small_mem_block_indices)) {
      return nullptr;
    }

    int num_small_in_mem = small_mem_block_indices.size();

    if (num_small_in_mem <= 0) {
      return nullptr;
    }
    //    int num_small_in_mem = 0;

    std::cout << "Small Size: " << small_n << std::endl;
    std::cout << "Large Size: " << large_n << std::endl;
    std::cout << "Num Small in Mem: " << num_small_in_mem << std::endl;
    std::cout << "Free In Mem: " << mManager.numFreeBlocks() << std::endl;

    //create condition evaluator with postfix expression and temp relation if not null postfix
    ConditionEvaluator eval;
    if(postFixExpr != nullptr)
      eval.initialize(postFixExpr, new_relation);

    if(!storeOutput) {
      printFieldNames(new_schema);
    }

    int small_done = 0;
    while (small_done < small_n) {
      int cur_small_in_mem = 0;
      for (int k = 0; small_done < small_n && k < num_small_in_mem; ++k) {
        small->getBlock(small_done, small_mem_block_indices[k]);
        small_done++;
        cur_small_in_mem++;
      }

      for(int i = 0; i < large_n; i++) {
        large->getBlock(i, large_mem_block_index);
        Block* large_mem_block = mem->getBlock(large_mem_block_index);

        for (int j = 0; j < cur_small_in_mem; ++j) {
          Block* small_mem_block = mem->getBlock(small_mem_block_indices[j]);

          std::vector<Tuple> large_tuples = large_mem_block->getTuples();
          std::vector<Tuple> small_tuples = small_mem_block->getTuples();

          for(int t1 = 0; t1 < large_tuples.size(); t1++) {
            if (large_tuples[t1].isNull()) {
              continue;
            }

            for(int t2 = 0; t2 < small_tuples.size(); t2++) {
              if (small_tuples[t2].isNull()) {
                continue;
              }

              Tuple new_tuple = new_relation->createTuple();
              for(unordered_map<int, int>::iterator it = small_to_new.begin(); it != small_to_new.end(); ++it) {
                int old_off = (*it).first;
                int new_off = (*it).second;
                FIELD_TYPE f = new_schema.getFieldType(new_off);
                if (f == INT) {
                  new_tuple.setField(new_off, small_tuples[t2].getField(old_off).integer);
                } else {
                  new_tuple.setField(new_off, *(small_tuples[t2].getField(old_off).str));
                }
              }

              for(unordered_map<int, int>::iterator it = large_to_new.begin(); it != large_to_new.end(); ++it) {
                int old_off = (*it).first;
                int new_off = (*it).second;
                FIELD_TYPE f = new_schema.getFieldType(new_off);
                if (f == INT) {
                  new_tuple.setField(new_off, large_tuples[t1].getField(old_off).integer);
                } else {
                  new_tuple.setField(new_off, *(large_tuples[t1].getField(old_off).str));
                }
              }

              bool ans = true;
              if(postFixExpr != nullptr)
                ans = eval.evaluate(new_tuple);

              if (ans) {
                if (storeOutput) {
                  appendTupleToRelation(new_relation, output_mem_block_index, new_tuple);
                } else {
                  std::cout << new_tuple << std::endl;
                }
              }
            }
          }
        }
      }
    }

    //memblocks release
    mManager.releaseBlock(large_mem_block_index);
    mManager.releaseBlock(output_mem_block_index);
    mManager.releaseNBlocks(small_mem_block_indices);

    if(storeOutput) {
      temp_relations.push_back(rNew);
      return new_relation;
    }
    return nullptr;
  }

  bool processSelectMultiTable(ParseTreeNode* root) {
    // create projection list

    // Break Where condition if only AND

    // For r1, r2 in relation_list
    // Extract WhereCondition for these two relations
    // if no more relations process with storeOutput = false
    // if more relations process with storeOutput = true
    // then r1 = newRelation
    // r2 = nextRelation

    // Drop temporary relations // create function for this
    return false;
  }

  bool processSelectStatement(ParseTreeNode* root) {
    int index = root->children[1]->type == NODE_TYPE::DISTINCT_LITERAL ? 4 : 3;
    if(root->children[index]->children.size() == 1) {
      std::vector<Tuple> tuples;
      return processSelectSingleTable(root, tuples, true);
    }
    else {
      Relation* newRelation;
      std::string r1 = "course";
      std::string r2 = "course2";

      unordered_map<std::string, bool> m;
      m["*"] = true;
      newRelation = crossJoinWithCondition(r1, r2, root->children[index + 2], m, false);
      // make logical query plan
    }
    return false;
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
