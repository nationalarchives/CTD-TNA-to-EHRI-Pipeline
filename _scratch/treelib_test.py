from treelib import Node, Tree

tree = Tree()
tree.create_node("Level 0", "root")  # root 
lineage = ["id1", "id2", "id3"]

for level, record_id in enumerate(lineage):
    current_parent = lineage[level - 1] if level > 0 else "root"
    tree.create_node(f"Level {level}", record_id, parent=current_parent)


tree.show()

