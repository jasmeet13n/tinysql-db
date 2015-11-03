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

class DatabaseManager {
private:
	MainMemory* mem;
	Disk* disk;
	SchemaManager schema_manager;

public:
	DatabaseManager(MainMemory* m, Disk* d);

	Relation* createTable(ParseTree root);

	bool dropTable(ParseTree root);
};
