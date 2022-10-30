from treelib import Node, Tree
import json

def read_json(path):
    f = open(path)
    res = json.load(f)
    return res;

def generate_operation_tree(plan):
    tree = Tree()

    queue = []
    node_id = 0
    queue.append((plan, node_id))
    tree.create_node(plan["Node Type"], node_id)
    node_id += 1

    while len(queue) > 0:
        node_tuple = queue.pop()
        cur_plan = node_tuple[0]
        cur_plan_id = node_tuple[1]
        if "Plans" in  cur_plan.keys():
            for subplan in cur_plan["Plans"]:
                queue.append((subplan, node_id))
                tree.create_node(subplan["Node Type"], node_id, parent = cur_plan_id)
                node_id += 1

    return tree


test = read_json("temp.json")
print(test)
tree = generate_operation_tree(test[0][0][0]["Plan"])
tree.show()