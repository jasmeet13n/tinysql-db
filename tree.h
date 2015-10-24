#ifndef _TREE_H
#define _TREE_H

#include <vector>

using namespace std;

class TreeNode {
	private:
		string value;
		std::vector<TreeNode> children;
	public:
		TreeNode(string val) {
			value = val;
		}
		void addChild(TreeNode child) {
			children.push_back(child);
		}
		string value() {
			return value;
		}
};

#endif