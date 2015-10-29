#include <iostream>
#include "DatabaseManager.h"

int main() {
	// Initialize the memory, disk, the database manager and the parser
	MainMemory mem;
	Disk disk;
	DatabaseManager db_manager(&mem, &disk);
}