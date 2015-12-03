#ifndef __MEMORY_MANAGER_INCLUDED
#define __MEMORY_MANAGER_INCLUDED

#include <stack>

#include "./StorageManager/Block.h"
#include "./StorageManager/Config.h"
#include "./StorageManager/Disk.h"
#include "./StorageManager/Field.h"
#include "./StorageManager/MainMemory.h"
#include "./StorageManager/Relation.h"
#include "./StorageManager/Schema.h"
#include "./StorageManager/SchemaManager.h"

class MemoryManager {
private:
  MainMemory* mem;
  stack<int> freeBlocks;
  int memory_size;

public:
  MemoryManager(MainMemory* m) {
    this->mem = m;
    memory_size = mem->getMemorySize();
    for (int i = memory_size - 1; i >= 0; --i) {
      freeBlocks.push(i);
    }
  }

  int numFreeBlocks() {
    return freeBlocks.size();
  }

  int getFreeBlockIndex() {
    if (!freeBlocks.empty()) {
      int top = freeBlocks.top();
      freeBlocks.pop();
      return top;
    }
    return -1;
  }

  bool getNFreeBlockIndices(vector<int>& ans, int n) {
    if (freeBlocks.size() < n) {
      return false;
    }

    if (ans.size() != n) {
      ans.resize(n, -1);
    }

    for (int i = 0; i < ans.size(); ++i) {
      ans[i] = freeBlocks.top();
      freeBlocks.pop();
    }

    return true;
  }

  bool getAllFreeBlockIndices(vector<int>& ans) {
    return getNFreeBlockIndices(ans, numFreeBlocks());
  }

  void releaseBlock(int i) {
    if (i == -1) {
      return;
    }
    freeBlocks.push(i);
  }

  void releaseNBlocks(vector<int>& blocks) {
    for (int i = 0; i < blocks.size(); ++i) {
      releaseBlock(blocks[i]);
    }
  }
};

#endif
