#ifndef __DB_MANAGER_INCLUDED
#define __DB_MANAGER_INCLUDED

#include <string>
#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <list>

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
  std::vector<std::string> tokens;

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

  bool appendTupleToMemBlock(Block* block_ptr, Tuple& tuple) {
    if(block_ptr->isFull()) {
      return false;
    } else {
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

  void removeTempRelations() {
    for(int i = 0; i < temp_relations.size(); i++) {
      schema_manager.deleteRelation(temp_relations[i]);
    }
    temp_relations.clear();
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
    Block* output_mem_block_ptr = nullptr;
    if (storeOutput) {
      output_mem_block_index = mManager.getFreeBlockIndex();
      if (output_mem_block_index == -1) {
        return nullptr;
      } else {
        output_mem_block_ptr = mem->getBlock(output_mem_block_index);
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
    // int num_small_in_mem = 0;

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
                  if (!appendTupleToMemBlock(output_mem_block_ptr, new_tuple)) {
                    new_relation->setBlock(new_relation->getNumOfBlocks(), output_mem_block_index);
                    output_mem_block_ptr->clear();
                    appendTupleToMemBlock(output_mem_block_ptr, new_tuple);
                  }
                } else {
                  std::cout << new_tuple << std::endl;
                }
              }
            }
          }
        }
      }
    }

    if (storeOutput && !(output_mem_block_ptr->isEmpty())) {
      new_relation->setBlock(new_relation->getNumOfBlocks(), output_mem_block_index);
      output_mem_block_ptr->clear();
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
    bool hasDistinct = hasDistinct = root->children[1]->type == NODE_TYPE::DISTINCT_LITERAL ? true : false;;
    bool hasOrderBy = false;
    bool hasDistOrSort = false;
    int selectListIndex = hasDistinct ? 2 : 1;

    bool hasWhereCondition = Utils::hasWhereCondition(root);
    ParseTreeNode* whereConditionRoot = hasWhereCondition ? root->children[selectListIndex + 4] : nullptr;

    std::vector<std::string> selectList;
    Utils::getSelectList(root, selectList);

    // create projection list
    std::unordered_map<std::string, bool> selectListMap;
    for (int i = 0; i < selectList.size(); ++i) {
      selectListMap[selectList[i]] = true;
    }

    std::string sortColName;
    if (hasWhereCondition) {
      if (selectListIndex + 5 < root->children.size()) {
        hasOrderBy = true;
        sortColName = root->children[selectListIndex + 7]->value;
      }
    } else if (selectListIndex + 3 < root->children.size()) {
      hasOrderBy = true;
      sortColName = root->children[selectListIndex + 5]->value;
    }

    if (hasOrderBy) {
      selectListMap[sortColName] = true;
    }

    std::list<ParseTreeNode*> whereConditions;

    bool foundOrCondition = false;
    if (hasWhereCondition) {
      int whereStart = 0;
      int whereEnd = tokens.size();
      for (int i = 0; i < tokens.size(); ++i) {
        if (tokens[i] == "WHERE") {
          whereStart = i+1;
          break;
        }
      }
      if (tokens[tokens.size() - 3] == "ORDER") {
        whereEnd -= 3;
      }

      for (int i = whereStart; i < whereEnd; ++i) {
        if (tokens[i] == "OR") {
          foundOrCondition = true;
          break;
        }
      }

      if (foundOrCondition) {
        whereConditions.push_back(whereConditionRoot);
      } else {
        int curStart = whereStart;
        for (int i = whereStart; i < whereEnd; ++i) {
          if (tokens[i] == "AND") {
            whereConditions.push_back(Parser::getPostfixNodePublic(tokens, curStart, i));
            curStart = i + 1;
          } else if (i == whereEnd - 1) {
            whereConditions.push_back(Parser::getPostfixNodePublic(tokens, curStart, whereEnd));
          }
        }
      }
    }

    if (hasDistinct || hasOrderBy) {
      hasDistOrSort = true;
    }

    // Create a vector of relation list
    std::vector<std::string> relationList;
    Utils::getTableList(root, relationList);


    std::unordered_map<std::string, Schema> relSchema;
    for (int i = 0; i < relationList.size(); ++i) {
      relSchema[relationList[i]] = schema_manager.getSchema(relationList[i]);
    }

    std::unordered_map<std::string, bool> allColumns;

    for (int i = 0; i < relationList.size(); ++i) {
      std::vector<std::string> fieldNames = relSchema[relationList[i]].getFieldNames();
      for (int j = 0; j < fieldNames.size(); ++j) {
        std::string col = relationList[i] + "." + fieldNames[j];
        allColumns[col] = true;
      }
    }

    for (auto it = whereConditions.begin(); it != whereConditions.end(); ++it) {
      ParseTreeNode* root = (*it);
      std::cout << "Print Current Tree--------->" << std::endl;
      ParseTreeNode::printParseTree(root);
      for (int i = 0; i < root->children.size(); ++i) {
        std::string& val = root->children[i]->value;
        if (allColumns.find(val) != allColumns.end()) {
          root->children[i]->type = NODE_TYPE::POSTFIX_VARIABLE;
        }
      }
    }

    std::string rel1 = relationList[0];
    std::unordered_map<std::string, bool> curColumns;
    std::vector<std::string> fieldNames;

    if (hasWhereCondition) {
      fieldNames = relSchema[rel1].getFieldNames();
      for (int j = 0; j < fieldNames.size(); ++j) {
        std::string col = rel1 + "." + fieldNames[j];
        curColumns[col] = true;
      }
    }

    for (int i = 1; i < relationList.size(); ++i) {
      std::string rel2 = relationList[i];

      ParseTreeNode* curWhereConditionRoot = nullptr;

      if (hasWhereCondition) {
        fieldNames = relSchema[rel2].getFieldNames();
        for (int j = 0; j < fieldNames.size(); ++j) {
          std::string col = rel2 + "." + fieldNames[j];
          curColumns[col] = true;
        }

        std::vector<ParseTreeNode*> curWhereConditions;
        for (auto it = whereConditions.begin(); it != whereConditions.end();) {
          ParseTreeNode* curRoot = (*it);
          bool pushThis = true;
          for (int j = 0; j < curRoot->children.size(); ++j) {
            if (curRoot->children[j]->type == NODE_TYPE::POSTFIX_VARIABLE) {
              if (curColumns.find(curRoot->children[j]->value) == curColumns.end()) {
                pushThis = false;
                break;
              }
            }
          }

          if (pushThis) {
            curWhereConditions.push_back(curRoot);
            it = whereConditions.erase(it);
          } else {
            ++it;
          }
        }

        if (curWhereConditions.size() > 0) {
          cout << "greater than zero" << endl;
          cout.flush();
          curWhereConditionRoot = curWhereConditions[0];
          for (int j = 1; j < curWhereConditions.size(); ++j) {
            ParseTreeNode* curRoot = curWhereConditions[j];
            for (int k = 0; k < curRoot->children.size(); ++k) {
              curWhereConditionRoot->children.push_back(curRoot->children[k]);
            }
            curWhereConditionRoot->children.push_back(new ParseTreeNode(NODE_TYPE::POSTFIX_OPERATOR, "AND"));
          }
        }
        ParseTreeNode::printParseTree(curWhereConditionRoot);
      }

      bool storeOutput = true;
      if (i == relationList.size() - 1 && !hasDistOrSort) {
        storeOutput = false;
      }

      std::unordered_map<std::string, bool> curProjListMap;
      if (selectListMap.find("*") != selectListMap.end()) {
        curProjListMap["*"] = true;
      } else {
        for (int j = 0; j < selectList.size(); ++j) {
          if (curColumns.find(selectList[j]) != curColumns.end()) {
            curProjListMap[selectList[j]] = true;
          }
        }
        for (auto it = whereConditions.begin(); it != whereConditions.end(); ++it) {
          ParseTreeNode* curRoot = (*it);
          for (int j = 0; j < curRoot->children.size(); ++j) {
            if (curRoot->children[j]->type == NODE_TYPE::POSTFIX_VARIABLE) {
              if (curColumns.find(curRoot->children[j]->value) != curColumns.end()) {
                curProjListMap[curRoot->children[j]->value] = true;
              }
            }
          }
        }
      }

      Relation* outputRelation = crossJoinWithCondition(rel1, rel2, curWhereConditionRoot, curProjListMap, storeOutput);
      std::cout << "Complete" << std::endl;

      if (storeOutput) {
        if (outputRelation == nullptr) {
          return false;
        }
        rel1 = outputRelation->getRelationName();
      }
    }

    if (hasDistOrSort) {
      // sort
    }

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
      //order by 
      if(root->children.size() > index + 1) {
        int order_index = root->children[index+1]->type == NODE_TYPE::WHERE_LITERAL ? index + 3 : index + 1;
        if(root->children.size() > order_index + 1) {
          processSelectSingleTable(root, tuples, false);
          int num_free_mem_blocks = mManager.numFreeBlocks();
          int num_blocks_req = tuples.size() / tuples[0].getTuplesPerBlock();
          //if(false) {
          if(num_blocks_req <= num_free_mem_blocks) {
            //can fit in main memory; fill up main_mem blocks
            int current_block_index = mManager.getFreeBlockIndex();
            Block* current_block = mem->getBlock(current_block_index);
            current_block->clear();
            std::vector<int> mem_block_indices;
            for(int i = 0; i < tuples.size(); i++) {
              if(!appendTupleToMemBlock(current_block, tuples[i])) {
                mem_block_indices.push_back(current_block_index);
                current_block_index = mManager.getFreeBlockIndex();
                current_block = mem->getBlock(current_block_index);
                current_block->clear();
                appendTupleToMemBlock(current_block, tuples[i]);
              }
            }
            mem_block_indices.push_back(current_block_index);
            Relation* temp_rel = sort(root->children[index]->children[0]->value, 
              root->children[order_index + 2]->value, mem_block_indices);
            for(int i = 0; i < mem_block_indices.size(); i++) {
              Block* block = mem->getBlock(mem_block_indices[i]);
              std::vector<Tuple> mem_tuples = block->getTuples();
              for(int j = 0; j < mem_tuples.size(); j++)
                std::cout<< mem_tuples[j]<< std::endl;
            }
            mManager.releaseNBlocks(mem_block_indices);
          }
          else {
            // can't fit in main memory
            std::vector<int> mem_blocks;
            Relation* temp_rel = sort(root->children[index]->children[0]->value, 
              root->children[order_index + 2]->value, mem_blocks);
            //mManager.releaseNBlocks(mem_blocks);
          }
          return true;
        }
      }
      return processSelectSingleTable(root, tuples, true);
    } else {
      return processSelectMultiTable(root);
      Relation* newRelation;
      std::string r1 = "course";
      std::string r2 = "course2";

      unordered_map<std::string, bool> m;
      m["*"] = true;
      newRelation = crossJoinWithCondition(r1, r2, root->children[index + 2], m, true);
      std::cout << newRelation << std::endl;
      // make logical query plan
    }
    return false;
  }



  //main sort function
  Relation* sort(std::string relation_name, std::string column_name, std::vector<int>& mem_block_indices) {
    Relation* ret_rel;
    //if(false) {
    if(mem_block_indices.size() > 0) {
      sortMemory(relation_name, column_name, mem_block_indices);
      return nullptr;
    }
    else {
      ret_rel = sortRelation(relation_name, column_name, mem_block_indices);
      if(mem_block_indices.size() > 0)
        return nullptr;
    }
    return ret_rel;
  }

  //sortMemory function
  void sortMemory(std::string relation_name, std::string column_name, std::vector<int>& mem_block_indices) {
    int blocks = mem_block_indices.size();
    int tuples_per_block = (mem->getBlock(mem_block_indices[0]))->getNumTuples(); //max tuples per block
    int j = 0;
    int n = (blocks - 1) * tuples_per_block;
    n = n + (mem->getBlock(mem_block_indices[mem_block_indices.size() - 1]))->getNumTuples();
    bool swapped = true;

    while(swapped) {
      swapped = false;
      for(int i =  0; i < n - 1 - j; i++) {
        int block_num_1 = i / tuples_per_block;
        int block_num_2 = (i + 1) / tuples_per_block;
        Block* block1 = mem->getBlock(mem_block_indices[block_num_1]);
        Block* block2 = mem->getBlock(mem_block_indices[block_num_2]);
        int tuple1_offset = i % tuples_per_block;
        int tuple2_offset = (i + 1) % tuples_per_block;
        Field field1 = (block1->getTuple(tuple1_offset)).getField(column_name);
        Field field2 = (block2->getTuple(tuple2_offset)).getField(column_name);
        Schema s = (block1->getTuple(tuple1_offset)).getSchema();
        if(compareFields(s.getFieldType(column_name), field1, field2) == 1) {
          Tuple temp = block1->getTuple(tuple1_offset);
          block1->setTuple(tuple1_offset, block2->getTuple(tuple2_offset));
          block2->setTuple(tuple2_offset, temp);
          swapped = true;
        }
      }
      j++;
    }
  }  

  //sortRelation function
  Relation* sortRelation(std::string relation_name, std::string column_name, std::vector<int>& mem_block_indices) {
    //mem_block_indices.size() == 0
    Relation* orig_rel = schema_manager.getRelation(relation_name);
    int rel_blocks = orig_rel->getNumOfBlocks();
    //if(false) {
    if(rel_blocks <= mManager.numFreeBlocks()) {
      for(int i = 0; i < rel_blocks; i++) {
        int free_block_index = mManager.getFreeBlockIndex();
        orig_rel->getBlock(i, free_block_index);
        mem_block_indices.push_back(free_block_index);
      }
      sortMemory(relation_name, column_name, mem_block_indices);
      return nullptr;
    }
    else { //two pass
      Schema schema = orig_rel->getSchema();
      Relation* sublist_rel = schema_manager.createRelation("sublist_rel", schema);
      Relation* final_rel = schema_manager.createRelation("final_rel", schema);
      temp_relations.push_back("sublist_rel");
      temp_relations.push_back("final_rel");
      int num_free_mem_blocks = mManager.numFreeBlocks();
      int rel_num_blocks = orig_rel->getNumOfBlocks();
      std::vector<int> sublist_indices;

      //create sublist_rel

      for(int i = 0; i < rel_num_blocks; i += num_free_mem_blocks) {
        //index of each sublist
        sublist_indices.push_back(i);
        std::vector<int> i_mem_block_indices;
        for(int j = i; j < rel_num_blocks && j < i + num_free_mem_blocks; j++) {
          int free_block_index = mManager.getFreeBlockIndex();
          i_mem_block_indices.push_back(free_block_index);
          orig_rel->getBlock(j, free_block_index);
        }
        //sort in memory
        sortMemory(relation_name, column_name, i_mem_block_indices);
        //write it to sublist_rel
        int index = 0;
        for(int j = i; j < rel_num_blocks && j < i + num_free_mem_blocks; j++) {
          sublist_rel->setBlock(j, i_mem_block_indices[index++]);
        }
        //release
        mManager.releaseNBlocks(i_mem_block_indices);
      }

      int sublist_to_mem_map[sublist_indices.size()];
      int sublist_counters[sublist_indices.size()];
      for(int i = 0; i < sizeof(sublist_counters); i++)
        sublist_counters[i] = 0;
      int tuple_index[sublist_indices.size()];
      for(int i = 0; i < sizeof(tuple_index); i++)
        tuple_index[i] = 0;

      int output_block_index = mManager.getFreeBlockIndex();
      Block* output = mem->getBlock(output_block_index);
      output->clear();
      int allDone = false;

      std::vector<Block*> mem_blocks_sublist;
    //bring 1 block from each sublist in memory
      for(int i = 0; i < sublist_indices.size(); i++) {
        int free_block_index = mManager.getFreeBlockIndex();
        mem_blocks_sublist.push_back(mem->getBlock(free_block_index));
        sublist_rel->getBlock(sublist_indices[i], free_block_index);
        sublist_to_mem_map[i] = free_block_index;
      }

      while(!allDone) {
      //find minimum tuple
        int min_tuple_block = 0;
        int min_tuple_index = 0;
        int tuple_temp_index = 0, block_temp_index = 0;
        Tuple min_tuple = mem_blocks_sublist[block_temp_index]->getTuple(tuple_temp_index);
        while(min_tuple.isNull()) {
          if(++tuple_temp_index == mem_blocks_sublist[block_temp_index]->getNumTuples()) {
            block_temp_index++;
            tuple_temp_index = 0;
          }
          if(block_temp_index == sublist_indices.size()) {
            allDone = true;
            break;
          }
          min_tuple = mem_blocks_sublist[block_temp_index]->getTuple(tuple_temp_index);
        }
        Schema s = mem_blocks_sublist[block_temp_index]->getTuple(tuple_temp_index).getSchema();

        std::cout<<"min tuple: " << min_tuple <<endl;

        if(allDone)
          break;

        for(int i = 0; i < mem_blocks_sublist.size(); i++) {
          for(int j = 0; j < mem_blocks_sublist[i]->getNumTuples(); j++) {
            Tuple tuple = mem_blocks_sublist[i]->getTuple(j);
            if(tuple.isNull())
              continue;
            Field field1 = min_tuple.getField(column_name);
            Field field2 = tuple.getField(column_name);
            if(compareFields(s.getFieldType(column_name), field1, field2) == 1) {
              min_tuple = tuple;
              min_tuple_block = i;
              min_tuple_index = j;
            }
          }
        }

        output->appendTuple(min_tuple);
        if(output->isFull()) {
          final_rel->setBlock(final_rel->getNumOfBlocks(), output_block_index);
          output->clear();
        }
        std::cout<< "final relation : " <<endl << *final_rel<<endl;
        
        mem_blocks_sublist[min_tuple_block]->nullTuple(min_tuple_index);
        int all_tuples_in_block_null = 1;
        for(int i = 0; i < mem_blocks_sublist[min_tuple_block]->getNumTuples(); i++) {
          if(!mem_blocks_sublist[min_tuple_block]->getTuple(i).isNull())
            all_tuples_in_block_null = 0;
        }
        if(all_tuples_in_block_null == 1) {
          sublist_counters[min_tuple_block]++;
        //if within limit
          if((min_tuple_block == sublist_indices.size() - 1 && 
            sublist_indices[min_tuple_block] + sublist_counters[min_tuple_block] < rel_num_blocks) ||
            (sublist_indices[min_tuple_block] + sublist_counters[min_tuple_block] < sublist_indices[min_tuple_block + 1])) {
            sublist_rel->getBlock(sublist_indices[min_tuple_block] + sublist_counters[min_tuple_block], min_tuple_block);
          }
        }
      }

      allDone = true;
      for(int i = 0; i < mem_blocks_sublist.size(); i++) {
        for(int j = 0; j < mem_blocks_sublist[i]->getNumTuples(); j++) {
          if(!mem_blocks_sublist[i]->getTuple(j).isNull()) {
            allDone = false;
            break;
          }
        }
      }
      return final_rel;
    }

    return nullptr;
  }


  bool processQuery(std::string& query) {
    disk->resetDiskIOs();
    disk->resetDiskTimer();

    bool result = false;

    std::cout << "Q>"<< query << std::endl;
    ParseTreeNode* root = Parser::parseQuery(query, tokens);
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
