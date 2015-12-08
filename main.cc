#include <iostream>
#include "DatabaseManager.cc"

int main() {
	// Initialize the memory, disk, the database manager
	MainMemory mem;
	Disk disk;
	DatabaseManager db_manager(&mem, &disk);

  std::string query;
	while (std::getline(std::cin, query)) {
	  if (query == "exit") {
	    break;
	  }
	  std::cout << (db_manager.processQuery(query) ? "SUCCESS" : "FAILURE") << std::endl;
	}
	return 0;
}
