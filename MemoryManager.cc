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

  int getFreeBlockIndex() {
    if (!freeBlocks.empty()) {
      int top = freeBlocks.top();
      freeBlocks.pop();
      return top;
    }
    return -1;
  }

  void releaseBlock(int i) {
    freeBlocks.push(i);
  }
};

#endif
