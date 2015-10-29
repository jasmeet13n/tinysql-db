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

using namespace std;

DatabaseManager::DatabaseManager(MainMemory* m, Disk* d) : schema_manager(m, d) {
		this->mem = m;
		this->disk = d;
}

Relation* DatabaseManager::createTable(ParseTree root) {
		//Extract Relation Name, Field Names and Field Types
		string relation_name = root.getRelationName();
		vector<string> field_names = root.getColumnNames();
  	vector<enum FIELD_TYPE> root.getDataTypes();
  	//Create schema and Create Relation
  	Schema schema(field_names,field_types);
    return schema_manager.createRelation(relation_name,schema);
}

bool DatabaseManager::dropTable(ParseTree root) {
		string relation_name = root.getRelationName();
		return schema_manager.deleteRelation(relation_name);
}