#ifndef _DDL_H
#define _DDL_H

#include <vector>
#include <sstream>
#include <iostream>
#include <string>
#include "./StorageManager/Field.h"

using namespace std;

class SchemaManager;  //must do forward declaration
class Schema;
class Relation;
class TreeNode;


void createTable(TreeNode root, const SchemaManager& schema_manager) {
    //create-table-statement ::= CREATE TABLE table-name(attribute-type-list)

    //root.children[2] is table-name
    string relation_name = root.children[2].children[0].value();
    vector<string> field_names;
    vector<enum FIELD_TYPE> field_types;
    
    //root.children[4] is attribute-type-list
    //root.children[4].children[0].value == the whole attribute-type-list as a string
    vector<String> sublist; //each entry in sublist is "column-name data-type"
    istringstream ss(root.children[4].children[0].value());
    string token;
    while(std::getline(ss, token, ',')) {
        size_t strBegin = token.find_first_not_of(" ");
        sublist.push_back(token.substr(strBegin));
    }
    
    for(int i = 0; i < sublist.size(); i++) {
        size_t pos = sublist[i].find_first_of(" ");
        field_names.push_back(sublist[i].substr(0, pos));
        field_types.push_back(sublist[i].substr(pos+1));
    }

    //Check whether this would work or not - object declaration inside function?
    Schema schema(field_names,field_types);
    Relation* relation_ptr = schema_manager.createRelation(relation_name,schema);
}



#endif
