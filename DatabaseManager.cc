#ifndef __DB_MANAGER_INCLUDED
#define __DB_MANAGER_INCLUDED

#include <string>
#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <list>
#include <queue>
#include <fstream>

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
  std::ofstream fout;

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

  void printAndLog(std::string str) {
    fout << str;
    cout << str;
  }

  void printAndLog(Tuple tuple) {
    tuple.printTuple(fout);
    tuple.printTuple(std::cout);
  }

public:
  DatabaseManager(MainMemory* m, Disk* d) : schema_manager(m, d), mManager(m) , fout("log.txt", std::ofstream::out) {
    this->mem = m;
    this->disk = d;
  }

  ~DatabaseManager() {
    fout.close();
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
      //processSelectSingleTable(root->children[4]->children[0], insert_tuples);
      ParseTreeNode* select_tree_root = root->children[4]->children[0];
      std::vector<int> returnMemBlockIndices;
      std::vector<std::string> att_list;
      Utils::getAttributeList(root, att_list);
      Schema s = r->getSchema();
      
      Relation *rel = processSelectMultiTable(select_tree_root, true, returnMemBlockIndices);
      if(rel == nullptr) {
        for(int i = 0; i < returnMemBlockIndices.size(); i++) {
          Block* block = mem->getBlock(returnMemBlockIndices[i]);
          std::vector<Tuple> tuples = block->getTuples();
          for(int j = 0; j < tuples.size(); j++) {
            Tuple t = r->createTuple();
            for (int k = 0; k < att_list.size(); ++k) {
              FIELD_TYPE f = s.getFieldType(att_list[k]);
              if(f == INT)
                t.setField(att_list[k], tuples[j].getField(att_list[k]).integer);
              else
                t.setField(att_list[k], *(tuples[j].getField(att_list[k]).str));
            }
            insert_tuples.push_back(t);
          }
        }
      }
      else {
        for(int i = 0; i < rel->getNumOfBlocks(); i++) {
          int free_block_index = mManager.getFreeBlockIndex();
          rel->getBlock(i, free_block_index);
          Block* mem_block = mem->getBlock(free_block_index);
          std::vector<Tuple> tuples = mem_block->getTuples();
          for(int j = 0; j < tuples.size(); j++) {
            Tuple t = r->createTuple();
            for (int k = 0; k < att_list.size(); ++k) {
              FIELD_TYPE f = s.getFieldType(att_list[k]);
              if(f == INT)
                t.setField(att_list[k], tuples[j].getField(att_list[k]).integer);
              else
                t.setField(att_list[k], *(tuples[j].getField(att_list[k]).str));
            }
            insert_tuples.push_back(t);
          }
        }
      }
    }

    return insertTuplesIntoTable(table_name, insert_tuples);
  }

  void removeTempRelations() {
    for(int i = 0; i < temp_relations.size(); i++) {
      schema_manager.deleteRelation(temp_relations[i]);
    }
    temp_relations.clear();
  }

  bool processDeleteStatement(ParseTreeNode* root) {
    std::string tableName = root->children[2]->value;
    Relation* rel = schema_manager.getRelation(tableName);

    if (root->children.size() == 3) {
      rel->deleteBlocks(0);
      return true;
    }

    ConditionEvaluator eval;
    eval.initialize(root->children[4], rel);

    int inMemBlockIndex = mManager.getFreeBlockIndex();
    int outMemBlockIndex = mManager.getFreeBlockIndex();
    if (inMemBlockIndex == -1 || outMemBlockIndex == -1) {
      return false;
    }
    Block* outMemBlockPtr = mem->getBlock(outMemBlockIndex);

    int writeBlockIndex = 0;
    int numBlocks = rel->getNumOfBlocks();
    for (int i = 0; i < numBlocks; ++i) {
      rel->getBlock(i, inMemBlockIndex);
      Block* inMemBlockPtr = mem->getBlock(inMemBlockIndex);
      std::vector<Tuple> tuples = inMemBlockPtr->getTuples();
      for (int j = 0; j < tuples.size(); ++j) {
        bool ans = true;
        ans = eval.evaluate(tuples[j]);

        if (!ans) {
          if(!appendTupleToMemBlock(outMemBlockPtr, tuples[j])) {
            rel->setBlock(writeBlockIndex, outMemBlockIndex);
            outMemBlockPtr->clear();
            appendTupleToMemBlock(outMemBlockPtr, tuples[j]);
            writeBlockIndex++;
          }
        }
      }
    }

    if (!outMemBlockPtr->isEmpty()) {
      rel->setBlock(writeBlockIndex, outMemBlockIndex);
      outMemBlockPtr->clear();
      writeBlockIndex++;
    }

    rel->deleteBlocks(writeBlockIndex);
    return true;
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
        printAndLog(tuples[j]);
        printAndLog("\n");
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

  std::pair<Schema, Schema> createCommonSchema(Relation* small, Relation* large,
      std::unordered_map<std::string, bool>& selectListMap,
      std::unordered_map<std::string, bool>& projListMap,
      std::unordered_map<int, int>& smallToIn,
      std::unordered_map<int, int>& largeToIn,
      std::unordered_map<int, int>& inToOut) {
    std::string smallRelationName = small->getRelationName();
    std::string largeRelationName = large->getRelationName();
    Schema smallSchema = schema_manager.getSchema(smallRelationName);
    Schema largeSchema = schema_manager.getSchema(largeRelationName);

    std::vector<std::string> smallColumnNames = smallSchema.getFieldNames();
    std::vector<enum FIELD_TYPE> smallDataTypes = smallSchema.getFieldTypes();
    std::vector<std::string> largeColumnNames = largeSchema.getFieldNames();
    std::vector<enum FIELD_TYPE> largeDataTypes = largeSchema.getFieldTypes();

    std::string dotString = ".";
    for(int  i = 0; i < smallColumnNames.size(); ++i) {
      if (smallColumnNames[i].find(dotString) == std::string::npos) {
        smallColumnNames[i] = smallRelationName + "." + smallColumnNames[i];
      }
    }

    for(int  i = 0; i < largeColumnNames.size(); ++i) {
      if (largeColumnNames[i].find(dotString) == std::string::npos) {
        largeColumnNames[i] = largeRelationName + "." + largeColumnNames[i];
      }
    }

    std::vector<std::string> inColumnNames;
    std::vector<enum FIELD_TYPE> inDataTypes;

    std::vector<std::string> outColumnNames;
    std::vector<enum FIELD_TYPE> outDataTypes;

    bool starPresent = false;
    if (projListMap.find("*") != projListMap.end()) {
      starPresent = true;
    }


    for(int i = 0; i < smallColumnNames.size(); ++i) {
      if(starPresent || selectListMap.find(smallColumnNames[i]) != selectListMap.end()) {
        inColumnNames.push_back(smallColumnNames[i]);
        inDataTypes.push_back(smallDataTypes[i]);
        smallToIn[i] = inColumnNames.size() - 1;
      }
      if(starPresent || projListMap.find(smallColumnNames[i]) != projListMap.end()) {
        outColumnNames.push_back(smallColumnNames[i]);
        outDataTypes.push_back(smallDataTypes[i]);
        inToOut[smallToIn[i]] = outColumnNames.size() - 1;
      }
    }

    for(int i = 0; i < largeColumnNames.size(); ++i) {
      if(starPresent || selectListMap.find(largeColumnNames[i]) != selectListMap.end()) {
        inColumnNames.push_back(largeColumnNames[i]);
        inDataTypes.push_back(largeDataTypes[i]);
        largeToIn[i] = inColumnNames.size() - 1;
      }
      if(starPresent || projListMap.find(largeColumnNames[i]) != projListMap.end()) {
        outColumnNames.push_back(largeColumnNames[i]);
        outDataTypes.push_back(largeDataTypes[i]);
        inToOut[largeToIn[i]] = outColumnNames.size() - 1;
      }
    }

    Schema inSchema(inColumnNames, inDataTypes);
    Schema outSchema(outColumnNames, outDataTypes);
    return std::make_pair(inSchema, outSchema);
  }

  void printFieldNames(Schema schema) {
    std::vector<std::string> field_names;
    field_names = schema.getFieldNames();
    for(int i = 0; i < field_names.size(); i++) {
      printAndLog(field_names[i]+"\t");
    }
    printAndLog("\n");
  }

  Relation* tableScanWithCondition(std::string& relName,
      ParseTreeNode* postFixExpr, std::vector<int>& returnMemBlockIndices,
      std::unordered_map<std::string, bool>& projListMap, bool storeOutput) {
    Relation* rel = schema_manager.getRelation(relName);
    int numBlocksInRel = rel->getNumOfBlocks();

    Schema curSchema = rel->getSchema();
    std::vector<std::string> curFieldNames = curSchema.getFieldNames();
    std::vector<enum FIELD_TYPE> curFieldTypes = curSchema.getFieldTypes();

    std::vector<std::string> outFieldNames;
    std::vector<enum FIELD_TYPE> outFieldTypes;

    vector<int> oldToOut;
    bool hasStar = false;
    if (projListMap.find("*") != projListMap.end()) {
      hasStar = true;
    }
    for (int i = 0; i < curFieldNames.size(); ++i) {
      if (hasStar || projListMap.find(curFieldNames[i]) != projListMap.end()) {
        oldToOut.push_back(i);
        outFieldNames.push_back(curFieldNames[i]);
        outFieldTypes.push_back(curFieldTypes[i]);
      }
    }

    Schema outSchema(outFieldNames, outFieldTypes);

    std::string outRelName = relName + "_out_tmp";
    Relation* outRel = schema_manager.createRelation(outRelName, outSchema);
    temp_relations.push_back(outRelName);
    relName = outRelName;

    bool allInMemory = true;
    std::vector<int> curMemBlockIndices;

    int inMemBlockIndex = mManager.getFreeBlockIndex();
    if (inMemBlockIndex == -1) {
      return nullptr;
    }
    Block* inMemBlockPtr = mem->getBlock(inMemBlockIndex);

    int curMemBlockIndex = mManager.getFreeBlockIndex();
    if (curMemBlockIndex == -1) {
      return nullptr;
    }
    Block* curMemBlockPtr = mem->getBlock(curMemBlockIndex);
    curMemBlockIndices.push_back(curMemBlockIndex);

    ConditionEvaluator eval;
    if (postFixExpr != nullptr) {
      eval.initialize(postFixExpr, rel);
    }

    if (!storeOutput) {
      printFieldNames(outSchema);
    }

    for (int i = 0; i < numBlocksInRel; ++i) {
      rel->getBlock(i, inMemBlockIndex);
      std::vector<Tuple> curTuples = inMemBlockPtr->getTuples();
      std::vector<Tuple> outTuples;

      for (int j = 0; j < curTuples.size(); ++j) {
        bool ans = true;
        // condition evaluator
        if (postFixExpr != nullptr) {
          ans = eval.evaluate(curTuples[j]);
        }

        if (ans) {
          Tuple outTuple = outRel->createTuple();
          for (int k = 0; k < oldToOut.size(); ++k) {
            if (curFieldTypes[oldToOut[k]] == INT) {
              outTuple.setField(k, curTuples[j].getField(oldToOut[k]).integer);
            } else {
              outTuple.setField(k, *(curTuples[j].getField(oldToOut[k]).str));
            }
          }
          outTuples.push_back(outTuple);
          // create new Tuple according to Projection List
          // push newTuple to newTuples
        }
      }

      for (int j = 0; j < outTuples.size(); ++j) {
        if (storeOutput) {
          if (!appendTupleToMemBlock(curMemBlockPtr, outTuples[j])) {
            curMemBlockIndex = mManager.getFreeBlockIndex();
            if (curMemBlockIndex == -1) {
              for (int k = 0; k < curMemBlockIndices.size(); ++k) {
                if (storeOutput) {
                  // output this block to newRelation
                } else {
                  Block* curBlock = mem->getBlock(curMemBlockIndices[k]);
                  std::vector<Tuple> tuplesInThisBlock = curBlock->getTuples();
                  for (int l = 0; l < tuplesInThisBlock.size(); ++l) {
                    // print the tuple
                  }
                }
              }
              allInMemory = false;
              mManager.releaseNBlocks(curMemBlockIndices);
              curMemBlockIndices.clear();
              curMemBlockIndex = mManager.getFreeBlockIndex();
              if (curMemBlockIndex == -1) {
                return nullptr;
              }
            }
            curMemBlockPtr = mem->getBlock(curMemBlockIndex);
            curMemBlockIndices.push_back(curMemBlockIndex);
            appendTupleToMemBlock(curMemBlockPtr, outTuples[j]);
          }
        } else {
          printAndLog(outTuples[j]);
          printAndLog("\n");
        }
      }
    }
    mManager.releaseBlock(inMemBlockIndex);

    if (storeOutput) {
      if (allInMemory) {
        for (int i = 0; i < curMemBlockIndices.size(); ++i) {
          returnMemBlockIndices.push_back(curMemBlockIndices[i]);
        }
        return nullptr;
      } else {
        mManager.releaseNBlocks(curMemBlockIndices);
        return rel;
      }
    } else {
      mManager.releaseNBlocks(curMemBlockIndices);
    }
    return nullptr;
  }


  Relation* crossJoinWithCondition(std::string rSmall, std::string rLarge,
      ParseTreeNode* postFixExpr, std::unordered_map<std::string, bool>& selectListMap,
      std::unordered_map<std::string, bool>& projListMap, bool storeOutput) {
    Relation* small = schema_manager.getRelation(rSmall);
    Relation* large = schema_manager.getRelation(rLarge);
    int small_n = small->getNumOfBlocks();
    int large_n = large->getNumOfBlocks();

    if (small_n > large_n) {
      std::swap(small_n, large_n);
      std::swap(small, large);
      std::swap(rSmall, rLarge);
    }

    Schema inputSchema;
    Schema outputSchema;

    //create new schema with projection list
    std::unordered_map<int, int> smallToIn;
    std::unordered_map<int, int> largeToIn;
    std::unordered_map<int, int> inToOut;
    std::pair<Schema, Schema> doubleS = createCommonSchema(small, large, selectListMap, projListMap, smallToIn, largeToIn, inToOut);

    Schema inSchema = doubleS.first;
    Schema outSchema = doubleS.second;

    //create new relation
    std::string rIn = rSmall + "_" + rLarge + "_select";
    Relation* inRelation = schema_manager.createRelation(rIn, inSchema);

    std::string rOut = rSmall + "_" + rLarge;
    Relation* outRelation = schema_manager.createRelation(rOut, outSchema);

    temp_relations.push_back(rIn);
    temp_relations.push_back(rOut);

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

    //create condition evaluator with postfix expression and temp relation if not null postfix
    ConditionEvaluator eval;
    if(postFixExpr != nullptr)
      eval.initialize(postFixExpr, inRelation);

    if(!storeOutput) {
      printFieldNames(outSchema);
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

              Tuple inTuple = inRelation->createTuple();
              for(unordered_map<int, int>::iterator it = smallToIn.begin(); it != smallToIn.end(); ++it) {
                int old_off = (*it).first;
                int new_off = (*it).second;
                FIELD_TYPE f = inSchema.getFieldType(new_off);
                if (f == INT) {
                  inTuple.setField(new_off, small_tuples[t2].getField(old_off).integer);
                } else {
                  inTuple.setField(new_off, *(small_tuples[t2].getField(old_off).str));
                }
              }

              for(unordered_map<int, int>::iterator it = largeToIn.begin(); it != largeToIn.end(); ++it) {
                int old_off = (*it).first;
                int new_off = (*it).second;
                FIELD_TYPE f = inSchema.getFieldType(new_off);
                if (f == INT) {
                  inTuple.setField(new_off, large_tuples[t1].getField(old_off).integer);
                } else {
                  inTuple.setField(new_off, *(large_tuples[t1].getField(old_off).str));
                }
              }

              bool ans = true;
              if(postFixExpr != nullptr)
                ans = eval.evaluate(inTuple);

              if (ans) {
                Tuple outTuple = outRelation->createTuple();
                for (auto it = inToOut.begin(); it != inToOut.end(); ++it) {
                  int in_off = (*it).first;
                  int out_off = (*it).second;
                  FIELD_TYPE f = outSchema.getFieldType(out_off);
                  if (f == INT) {
                    outTuple.setField(out_off, inTuple.getField(in_off).integer);
                  } else {
                    outTuple.setField(out_off, *(inTuple.getField(in_off).str));
                  }
                }
                if (storeOutput) {
                  if (!appendTupleToMemBlock(output_mem_block_ptr, outTuple)) {
                    outRelation->setBlock(outRelation->getNumOfBlocks(), output_mem_block_index);
                    output_mem_block_ptr->clear();
                    appendTupleToMemBlock(output_mem_block_ptr, outTuple);
                  }
                } else {
                  printAndLog(outTuple);
                  printAndLog("\n");
                }
              }
            }
          }
        }
      }
    }

    if (storeOutput && !(output_mem_block_ptr->isEmpty())) {
      outRelation->setBlock(outRelation->getNumOfBlocks(), output_mem_block_index);
      output_mem_block_ptr->clear();
    }

    //memblocks release
    mManager.releaseBlock(large_mem_block_index);
    mManager.releaseBlock(output_mem_block_index);
    mManager.releaseNBlocks(small_mem_block_indices);

    if(storeOutput) {
      return outRelation;
    }
    return nullptr;
  }

  Relation* processSelectMultiTable(ParseTreeNode* root, bool globalStoreOutput, std::vector<int>& emptyMemBlocks) {
    bool hasDistinct = hasDistinct = root->children[1]->type == NODE_TYPE::DISTINCT_LITERAL ? true : false;;
    bool hasOrderBy = false;
    bool hasDistOrSort = false;
    int selectListIndex = hasDistinct ? 2 : 1;

    bool hasWhereCondition = Utils::hasWhereCondition(root);
    ParseTreeNode* whereConditionRoot = hasWhereCondition ? root->children[selectListIndex + 4] : nullptr;

    std::vector<std::string> selectList;
    Utils::getSelectList(root, selectList);

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
      selectList.push_back(sortColName);
    }

    std::unordered_map<std::string, bool> selectListMap;
    for (int i = 0; i < selectList.size(); ++i) {
      selectListMap[selectList[i]] = true;
    }

    std::list<ParseTreeNode*> whereConditions;

    // Create a vector of relation list
    std::vector<std::string> relationList;
    Utils::getTableList(root, relationList);
    std::vector<std::pair<int, std::string> > relPair;

    for(int i = 0; i < relationList.size(); i++) {
      relPair.push_back(std::make_pair(schema_manager.getRelation(relationList[i])->getNumOfTuples(), std::string(relationList[i])));
    }
    std::sort(relPair.begin(), relPair.end());

    for(int i = 0; i < relationList.size(); i++) {
      relationList[i] = relPair[i].second;
    }

    bool foundOrCondition = false;
    if (hasWhereCondition && relationList.size() > 1) {
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

    Relation* returnPtr = nullptr;

    if (hasWhereCondition && relationList.size() > 1) {
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
          curWhereConditionRoot = curWhereConditions[0];
          for (int j = 1; j < curWhereConditions.size(); ++j) {
            ParseTreeNode* curRoot = curWhereConditions[j];
            for (int k = 0; k < curRoot->children.size(); ++k) {
              curWhereConditionRoot->children.push_back(curRoot->children[k]);
            }
            curWhereConditionRoot->children.push_back(new ParseTreeNode(NODE_TYPE::POSTFIX_OPERATOR, "AND"));
          }
        }
        //        ParseTreeNode::printParseTree(curWhereConditionRoot);
      }

      bool storeOutput = true;
      if (globalStoreOutput == true) {
        // Do nothing
      } else if (i == relationList.size() - 1 && !hasDistOrSort) {
        storeOutput = false;
      }

      std::unordered_map<std::string, bool> curProjListMap;
      std::unordered_map<std::string, bool> curSelectListMap;
      if (selectListMap.find("*") != selectListMap.end()) {
        curProjListMap["*"] = true;
        curSelectListMap["*"] = true;
      } else {
        for (int j = 0; j < selectList.size(); ++j) {
          if (curColumns.find(selectList[j]) != curColumns.end()) {
            curProjListMap[selectList[j]] = true;
            curSelectListMap[selectList[j]] = true;
          }
        }

        for (auto it = whereConditions.begin(); it != whereConditions.end(); ++it) {
          ParseTreeNode* curRoot = (*it);
          for (int j = 0; j < curRoot->children.size(); ++j) {
            if (curRoot->children[j]->type == NODE_TYPE::POSTFIX_VARIABLE) {
              if (curColumns.find(curRoot->children[j]->value) != curColumns.end()) {
                curSelectListMap[curRoot->children[j]->value] = true;
                curProjListMap[curRoot->children[j]->value] = true;
              }
            }
          }
        }

        ParseTreeNode* curRoot = curWhereConditionRoot;
        for (int j = 0; j < curRoot->children.size(); ++j) {
          if (curRoot->children[j]->type == NODE_TYPE::POSTFIX_VARIABLE) {
            if (curColumns.find(curRoot->children[j]->value) != curColumns.end()) {
              curSelectListMap[curRoot->children[j]->value] = true;
            }
          }
        }
      }

      returnPtr = crossJoinWithCondition(rel1, rel2, curWhereConditionRoot, curSelectListMap, curProjListMap, storeOutput);

      if (storeOutput) {
        if (returnPtr == nullptr) {
          return nullptr;
        }
        rel1 = returnPtr->getRelationName();
      }
    }

    bool storeOutput = false;
    if (globalStoreOutput == true) {
      storeOutput = true;
    } else if (hasDistOrSort) {
      storeOutput = true;
    }
    if (relationList.size() == 1) {
      std::unordered_map<std::string, bool> curProjListMap;
      for (int j = 0; j < selectList.size(); ++j) {
        std::string dotString = ".";
        std::size_t found = selectList[j].find(dotString);
        if (found != std::string::npos) {
          selectList[j] = selectList[j].substr(found + 1);
        }
        curProjListMap[selectList[j]] = true;
      }
      returnPtr = tableScanWithCondition(rel1, whereConditionRoot, emptyMemBlocks, curProjListMap, storeOutput);
      if (returnPtr != nullptr) {
        rel1 = returnPtr->getRelationName();
      }
    }

    if (hasDistOrSort) {
      if (hasDistinct && hasOrderBy) {
        returnPtr = removeDuplicates(rel1, sortColName, emptyMemBlocks, !globalStoreOutput);
      } else if (hasOrderBy) {
        returnPtr = ourSort(rel1, sortColName, emptyMemBlocks, !globalStoreOutput);
      } else if (hasDistinct) {
        returnPtr = removeDuplicates(rel1, schema_manager.getRelation(rel1)->getSchema().getFieldName(0), emptyMemBlocks, !globalStoreOutput);
      }
    }

    return returnPtr;
  }

  bool processSelectStatement(ParseTreeNode* root) {
    std::vector<int> dummyBlocks;
    Relation* rel = processSelectMultiTable(root, false, dummyBlocks);
    return true;
  }

  //assuming tuples have same schema
  bool equalTuples(Tuple tuple1, Tuple tuple2) {
    Schema s = tuple1.getSchema();
    std::vector<std::string> column_names = s.getFieldNames();
    for(int i = 0; i < column_names.size(); i++) {
      enum FIELD_TYPE f = s.getFieldType(column_names[i]);
      if(f == INT) {
        if(tuple1.getField(i).integer != tuple2.getField(i).integer)
          return false;
      }
      else {
        if(*tuple1.getField(i).str != *tuple2.getField(i).str)
          return false;
      }
    }
    return true;
  }

  bool equalFields(FIELD_TYPE f, Field field1, Field field2) {
    if(f == INT) {
      if(field1.integer != field2.integer)
        return false;
    }
    else {
      if(*field1.str != *field2.str)
        return false;
    }
    return true;
  }

  std::string convertTupleToString(Tuple tuple) {
    Schema s = tuple.getSchema();
    std::string result = "";
    std::vector<std::string> column_names = s.getFieldNames();
    for(int i = 0; i < column_names.size(); i++) {
      enum FIELD_TYPE f = s.getFieldType(column_names[i]);
      if(f == INT) {
        result += std::to_string(tuple.getField(column_names[i]).integer)+"_";
      }
      else {
        result += *(tuple.getField(column_names[i]).str)+"_";
      }
    }
    return result;
  }

  //main removeDuplicates function
  Relation* removeDuplicates(std::string relation_name, std::string column_name, std::vector<int>& mem_block_indices, bool print) {
    Relation* ret_rel;
    if(print)
      printFieldNames(schema_manager.getRelation(relation_name)->getSchema());
    //if(false) {
    if(mem_block_indices.size() > 0) {
      ret_rel = removeDuplicatesMemory(relation_name, column_name, mem_block_indices, print);
      if(!print)
        return ret_rel;
    }
    else {
      ret_rel = removeDuplicatesRelation(relation_name, column_name, mem_block_indices, print);
      if(!print)
        return ret_rel;
    }
    return nullptr;
  }

  //removeDuplicates in memory function
  Relation* removeDuplicatesMemory(std::string relation_name, std::string column_name, std::vector<int>& mem_block_indices, bool print) {
    Relation* ret_rel;
    sortMemory(relation_name, column_name, mem_block_indices, false);
    int output_block_index = mManager.getFreeBlockIndex();
    if(output_block_index == -1)
      return nullptr;
    std::unordered_set<std::string> seen_distinct_tuples;
    Block* output = mem->getBlock(output_block_index);
    Block* mem_block_0 = mem->getBlock(mem_block_indices[0]);
    Tuple tuple = mem_block_0->getTuple(0);
    seen_distinct_tuples.insert(convertTupleToString(tuple));
    Schema s = tuple.getSchema();
    union Field cur_comparing_col = tuple.getField(column_name);

    if(!print) {
      output->appendTuple(tuple);
    }
    else {
      printAndLog(tuple);
      printAndLog("\n");
    }

    {
      std::vector<Tuple> tuples = mem_block_0->getTuples(); 
      for(int j = 1; j < tuples.size(); j++) {
        Tuple tuple2 = mem_block_0->getTuple(j);
        bool seen = false;
        std::string converted_tuple = convertTupleToString(tuple2);
        if(seen_distinct_tuples.find(converted_tuple) != seen_distinct_tuples.end())
          seen = true;
        if(!seen) {
          tuple = tuple2;
          if(!equalFields(s.getFieldType(column_name), cur_comparing_col, tuple.getField(column_name))) {
            cur_comparing_col = tuple.getField(column_name);
            seen_distinct_tuples.clear();
          }
          seen_distinct_tuples.insert(converted_tuple);
          if(!print) {
            if(output->isFull()) {
              ret_rel->setBlock(ret_rel->getNumOfBlocks(), output_block_index);
              output->clear();
            }
            output->appendTuple(tuple);
          }
          else {
            printAndLog(tuple);
            printAndLog("\n");
          }
        }
      }
    }
    for(int i = 1; i < mem_block_indices.size(); i++) {
      Block* mem_block = mem->getBlock(mem_block_indices[i]);
      std::vector<Tuple> tuples = mem_block->getTuples();
      for(int j = 0; j < tuples.size(); j++) {
        Tuple tuple2 = mem_block->getTuple(j);
        bool seen = false;
        std::string converted_tuple = convertTupleToString(tuple2);
        if(seen_distinct_tuples.find(converted_tuple) != seen_distinct_tuples.end())
          seen = true;
        if(!seen) {
          tuple = tuple2;
          if(!equalFields(s.getFieldType(column_name), cur_comparing_col, tuple.getField(column_name))) {
            cur_comparing_col = tuple.getField(column_name);
            seen_distinct_tuples.clear();
          }
          seen_distinct_tuples.insert(converted_tuple);
          if(!print) {
            if(output->isFull()) {
              ret_rel->setBlock(ret_rel->getNumOfBlocks(), output_block_index);
              output->clear();
            }
            output->appendTuple(tuple);
          }
          else {
            printAndLog(tuple);
            printAndLog("\n");
          }
        }
      }
    }
    if(!print)
      return ret_rel;
    return nullptr;
  }

  class HeapElement {
  public:
    Field field;
    enum FIELD_TYPE field_type;
    int block_index;
    int tuple_index;

    HeapElement(Field f, enum FIELD_TYPE f_t, int b_i, int t_i) {
      field = f;
      field_type = f_t;
      block_index = b_i;
      tuple_index = t_i;
    }

  };

  class myCompare {
  public:
    bool operator() (const HeapElement* element1, const HeapElement* element2) {
      if(element1->field_type == INT) {
        return element1->field.integer > element2->field.integer;
      }
      return *element1->field.str > *element2->field.str;
    }
  };

  Relation* removeDuplicatesRelationTwoPass(std::string relation_name, std::string column_name, bool print) {
    Relation* orig_rel = schema_manager.getRelation(relation_name);
    Schema schema = orig_rel->getSchema();
    Relation* sublist_rel = schema_manager.createRelation("sublist_rel", schema);
    Relation* final_rel = schema_manager.createRelation("final_rel", schema);
    temp_relations.push_back("sublist_rel");
    temp_relations.push_back("final_rel");
    int num_free_mem_blocks = mManager.numFreeBlocks();
    int rel_num_blocks = orig_rel->getNumOfBlocks();
    std::vector<std::queue<int>> sublists;
    int field_offset = schema.getFieldOffset(column_name);
    enum FIELD_TYPE f_type = schema.getFieldType(field_offset);

    std::unordered_set<std::string> seen_distinct_tuples;
    union Field cur_comparing_col;

    //create sublists and sublist_rel
    for(int i = 0; i < rel_num_blocks; i += num_free_mem_blocks) {
      std::queue<int> curSublist;
      std::vector<int> i_mem_block_indices;
      for(int j = i; j < rel_num_blocks && j < i + num_free_mem_blocks; j++) {
        int free_block_index = mManager.getFreeBlockIndex();
        i_mem_block_indices.push_back(free_block_index);
        orig_rel->getBlock(j, free_block_index);
      }
      //sort in memory
      sortMemory(relation_name, column_name, i_mem_block_indices, false);
      //write it to sublist_rel
      int index = 0;
      for(int j = i; j < rel_num_blocks && j < i + num_free_mem_blocks; j++) {
        sublist_rel->setBlock(j, i_mem_block_indices[index++]);
        curSublist.push(j);
      }
      sublists.push_back(curSublist);
      //release
      mManager.releaseNBlocks(i_mem_block_indices);
    }

    //clear whole memory
    for(int i = 0; i < mem->getMemorySize(); i++)
      mem->getBlock(i)->clear();

    int output_block_index = mManager.getFreeBlockIndex();
    Block* output = mem->getBlock(output_block_index);

    //memory block index to sublist index map
    std::unordered_map<int, int> mem_to_sublist;

    //vector for heap
    std::vector<HeapElement*> heap;

    //vector of memory blocks used for sublist's block
    std::vector<int> mem_blocks_sublist;

    //bring 1 block from each sublist in memory
    for(int i = 0; i < sublists.size(); i++) {
      int free_block_index = mManager.getFreeBlockIndex();
      mem_blocks_sublist.push_back(free_block_index);
      sublist_rel->getBlock(sublists[i].front(), free_block_index);
      sublists[i].pop();
      mem_to_sublist[free_block_index] = i;
    }

    //create heap of first tuples from each block
    for(int i = 0; i < mem_blocks_sublist.size(); i++) {
      Block* block = mem->getBlock(mem_blocks_sublist[i]);
      Tuple tuple = block->getTuple(0);
      heap.push_back(new HeapElement(tuple.getField(field_offset), f_type, mem_blocks_sublist[i], 0));
    }

    std::make_heap(heap.begin(), heap.end(), myCompare());

    cur_comparing_col = heap.front()->field;

    while(heap.size() > 0) {
      HeapElement* current = heap.front();
      pop_heap(heap.begin(), heap.end(), myCompare());
      heap.pop_back();

      int current_block_index = current->block_index;
      int current_tuple_index = current->tuple_index;
      Block* mem_block = mem->getBlock(current_block_index);
      Tuple tuple = mem_block->getTuple(current_tuple_index);

      int seen = false;
      std::string converted_tuple = convertTupleToString(tuple);
      if(!seen_distinct_tuples.empty() && seen_distinct_tuples.find(converted_tuple) != seen_distinct_tuples.end())
        seen = true;
      if(!seen) {
        if(!equalFields(f_type, cur_comparing_col, tuple.getField(field_offset))) {
          cur_comparing_col = tuple.getField(field_offset);
          seen_distinct_tuples.clear();
        }
        output->appendTuple(tuple);
        seen_distinct_tuples.insert(converted_tuple);
      }
      //output->appendTuple(tuple);
      if(output->isFull()) {
        if(!print) {
          final_rel->setBlock(final_rel->getNumOfBlocks(), output_block_index);
        }
        else {
          std::vector<Tuple> output_tuples = output->getTuples();
          for(int i = 0; i < output_tuples.size(); i++) {
            printAndLog(output_tuples[i]);
            printAndLog("\n");
          }
        }
        output->clear();
      }

      //check block done
      if(current_tuple_index == mem_block->getNumTuples() - 1) {
        //if sublist not empty
        int sublist_index = mem_to_sublist[current_block_index];
        if(!sublists[sublist_index].empty()) {
          mem_block->clear();
          sublist_rel->getBlock(sublists[sublist_index].front(), current_block_index);
          sublists[sublist_index].pop();
          mem_block = mem->getBlock(current_block_index);
          heap.push_back(new HeapElement(mem_block->getTuple(0).getField(field_offset), f_type, current_block_index, 0));
          push_heap(heap.begin(), heap.end(), myCompare());
        }
      } else {
        //push another tuple in heap
        heap.push_back(new HeapElement(mem_block->getTuple(current_tuple_index + 1).getField(field_offset), f_type, current_block_index, current_tuple_index + 1));
        push_heap(heap.begin(), heap.end(), myCompare());
      }

    }

    return final_rel;
  }

  //removeDuplicates for relation function
  Relation* removeDuplicatesRelation(std::string relation_name, std::string column_name, std::vector<int>& mem_block_indices, bool print) {
    Relation* orig_rel = schema_manager.getRelation(relation_name);
    std::unordered_set<std::string> seen_distinct_tuples;
    union Field cur_comparing_col;
    int rel_blocks = orig_rel->getNumOfBlocks();
    //if(false) {
    if(rel_blocks <= mManager.numFreeBlocks()) {
      for(int i = 0; i < rel_blocks; i++) {
        int free_block_index = mManager.getFreeBlockIndex();
        orig_rel->getBlock(i, free_block_index);
        mem_block_indices.push_back(free_block_index);
      }
      Relation* ret_rel = removeDuplicatesMemory(relation_name, column_name, mem_block_indices, print);
      if(!print)
        return ret_rel;
      return nullptr;
    }
    else { //two pass
      return removeDuplicatesRelationTwoPass(relation_name, column_name, print);
    }
  }

  //main sort function
  Relation* ourSort(std::string relation_name, std::string column_name, std::vector<int>& mem_block_indices, bool print) {
    Relation* ret_rel;
    if(print)
      printFieldNames(schema_manager.getRelation(relation_name)->getSchema());
    //if(false) {
    if(mem_block_indices.size() > 0) {
      sortMemory(relation_name, column_name, mem_block_indices, print);
      return nullptr;
    }
    else {
      ret_rel = sortRelation(relation_name, column_name, mem_block_indices, print);
      if(mem_block_indices.size() > 0)
        return nullptr;
    }
    return ret_rel;
  }

  //sortMemory function
  void sortMemory(std::string relation_name, std::string column_name, std::vector<int>& mem_block_indices, bool print) {
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
    if(print) {
      for(int i = 0; i < mem_block_indices.size(); i++) {
        Block* block = mem->getBlock(mem_block_indices[i]);
        std::vector<Tuple> tuples = block->getTuples();
        for(int j = 0; j < tuples.size(); j++) {
          printAndLog(tuples[j]);
          printAndLog("\n");
        }
      }
    }
  }

  Relation* sortRelationTwoPass(std::string relation_name, std::string column_name, bool print) {
    Relation* orig_rel = schema_manager.getRelation(relation_name);
    Schema schema = orig_rel->getSchema();
    Relation* sublist_rel = schema_manager.createRelation("sublist_rel", schema);
    Relation* final_rel = schema_manager.createRelation("final_rel", schema);
    temp_relations.push_back("sublist_rel");
    temp_relations.push_back("final_rel");
    int num_free_mem_blocks = mManager.numFreeBlocks();
    int rel_num_blocks = orig_rel->getNumOfBlocks();
    std::vector<std::queue<int>> sublists;
    int field_offset = schema.getFieldOffset(column_name);
    enum FIELD_TYPE f_type = schema.getFieldType(field_offset);

    //create sublists and sublist_rel
    for(int i = 0; i < rel_num_blocks; i += num_free_mem_blocks) {
      std::queue<int> curSublist;
      std::vector<int> i_mem_block_indices;
      for(int j = i; j < rel_num_blocks && j < i + num_free_mem_blocks; j++) {
        int free_block_index = mManager.getFreeBlockIndex();
        i_mem_block_indices.push_back(free_block_index);
        orig_rel->getBlock(j, free_block_index);
      }
      //sort in memory
      sortMemory(relation_name, column_name, i_mem_block_indices, false);
      //write it to sublist_rel
      int index = 0;
      for(int j = i; j < rel_num_blocks && j < i + num_free_mem_blocks; j++) {
        sublist_rel->setBlock(j, i_mem_block_indices[index++]);
        curSublist.push(j);
      }
      sublists.push_back(curSublist);
      //release
      mManager.releaseNBlocks(i_mem_block_indices);
    }

    //clear whole memory
    for(int i = 0; i < mem->getMemorySize(); i++)
      mem->getBlock(i)->clear();

    int output_block_index = mManager.getFreeBlockIndex();
    Block* output = mem->getBlock(output_block_index);

    //memory block index to sublist index map
    std::unordered_map<int, int> mem_to_sublist;

    //vector for heap
    std::vector<HeapElement*> heap;

    //vector of memory blocks used for sublist's block
    std::vector<int> mem_blocks_sublist;

    //bring 1 block from each sublist in memory
    for(int i = 0; i < sublists.size(); i++) {
      int free_block_index = mManager.getFreeBlockIndex();
      mem_blocks_sublist.push_back(free_block_index);
      sublist_rel->getBlock(sublists[i].front(), free_block_index);
      sublists[i].pop();
      mem_to_sublist[free_block_index] = i;
    }

    //create heap of first tuples from each block
    for(int i = 0; i < mem_blocks_sublist.size(); i++) {
      Block* block = mem->getBlock(mem_blocks_sublist[i]);
      Tuple tuple = block->getTuple(0);
      heap.push_back(new HeapElement(tuple.getField(field_offset), f_type, mem_blocks_sublist[i], 0));
    }

    std::make_heap(heap.begin(), heap.end(), myCompare());

    while(heap.size() > 0) {
      HeapElement* current = heap.front();
      pop_heap(heap.begin(), heap.end(), myCompare());
      heap.pop_back();

      int current_block_index = current->block_index;
      int current_tuple_index = current->tuple_index;
      Block* mem_block = mem->getBlock(current_block_index);
      Tuple tuple = mem_block->getTuple(current_tuple_index);

      output->appendTuple(tuple);
      if(output->isFull()) {
        if(!print) {
          final_rel->setBlock(final_rel->getNumOfBlocks(), output_block_index);
        }
        else {
          std::vector<Tuple> output_tuples = output->getTuples();
          for(int i = 0; i < output_tuples.size(); i++) {
            printAndLog(output_tuples[i]);
            printAndLog("\n");
          }
        }
        output->clear();
      }

      //check block done
      if(current_tuple_index == mem_block->getNumTuples() - 1) {
        //if sublist not empty
        int sublist_index = mem_to_sublist[current_block_index];
        if(!sublists[sublist_index].empty()) {
          mem_block->clear();
          sublist_rel->getBlock(sublists[sublist_index].front(), current_block_index);
          sublists[sublist_index].pop();
          mem_block = mem->getBlock(current_block_index);
          heap.push_back(new HeapElement(mem_block->getTuple(0).getField(field_offset), f_type, current_block_index, 0));
          push_heap(heap.begin(), heap.end(), myCompare());
        }
      } else {
        //push another tuple in heap
        heap.push_back(new HeapElement(mem_block->getTuple(current_tuple_index + 1).getField(field_offset), f_type, current_block_index, current_tuple_index + 1));
        push_heap(heap.begin(), heap.end(), myCompare());
      }

    }

    return final_rel;
  }

  //sortRelation function
  Relation* sortRelation(std::string relation_name, std::string column_name, std::vector<int>& mem_block_indices, bool print) {
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
      sortMemory(relation_name, column_name, mem_block_indices, print);
      return nullptr;
    }
    else { //two pass
      return sortRelationTwoPass(relation_name, column_name, print);
    }
  }

  bool processQuery(std::string& query) {
    disk->resetDiskIOs();
    disk->resetDiskTimer();

    bool result = false;

    printAndLog("********************************************\n");
    printAndLog("Q>"+query+"\n");
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

    printAndLog("Disk I/O: " + std::to_string(disk->getDiskIOs()) + "\n");
    printAndLog("Execution Time: " + std::to_string(disk->getDiskTimer()) + " ms\n");

    mManager.releaseAllBlocks();
    removeTempRelations();
    return result;
  }
};

#endif
