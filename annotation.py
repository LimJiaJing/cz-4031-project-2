#Test

from treelib import Node, Tree
from deepdiff import DeepDiff
import json
import re
import preprocessing



def read_json(path):
    f = open(path)
    res = json.load(f)
    return res;

def parse_cond(raw):
    raw = re.sub(r'(.*\.)?(.*) = (.*\.)?(.*)', r'\2 = \4', raw)
    return raw

def generate_annotation(dic):
    annotations = {}

    joins = ["Hash Join", "Merge Join", "Index-based Join"]
    scans = ["Seq Scan", "Index Scan", "Index Only Scan"]

    for pair in dic.items():
        explanation = []
        query_component = pair[0][0]
        qep_tuple = pair[1][0]
        qep_algo, qep_cost = qep_tuple[0], qep_tuple[1]

        if qep_algo in scans:
            explanation.append("The table is read using {} with cost = {}.".format(qep_algo.lower(), qep_cost))
        elif qep_algo in joins:
            explanation.append("This join is implemented using {} with cost = {}.".format(qep_algo.lower(), qep_cost))
        else:
            explanation.append("This operation is implemented using {} with cost = {}.".format(qep_algo.lower(), qep_cost))

        if len(pair[1]) == 1:
            explanation.append("There is no alternative way for this operation.")
        else:
            aqp_tuples = pair[1][1:]

            # if qep_algo == "Seq Scan":
            #     explanation.append("The table is read using sequential scan because no index is available.")
            # el
            explanation.append(compare_plan(qep_tuple, aqp_tuples))
        # for aqp_tuple in pair[1][1:]:
        #     if qep_tuple[0] != aqp_tuple[0]:
        #         explanation.append(compare(qep_tuple, aqp_tuple))
        annotations[pair[0][1]] = " ".join(explanation)
    return annotations


def compare_plan(qep_tuple, aqp_tuples):
    qep_algo, qep_cost = qep_tuple[0], qep_tuple[1]
    explanation = []
    diff_algos = []

    for aqp_tuple in aqp_tuples:
        aqp_algo, aqp_cost = aqp_tuple[0], aqp_tuple[1]
        if not qep_algo == aqp_algo and aqp_algo not in diff_algos:
            diff_algos.append(aqp_algo)
            if qep_cost < aqp_cost:
                explanation.append("{} is not used because its cost is {}, " \
                   "{} times larger than {}.".format(aqp_algo, aqp_cost, round(aqp_cost / qep_cost), qep_algo))
            else:
                explanation.append("Although {} has lower cost ({})," \
                                   " DBMS still choose to use {} with a higher cost ({}) "
                                   "for some other consideration".format(aqp_algo, aqp_cost, qep_algo,
                                                                     qep_cost))
    if len(diff_algos) == 0:
        if (qep_algo == "Seq Scan"):
            explanation.append("There is no index available.")
        explanation.append("There is no alternative way for this operation.")
    return " ".join(explanation)


def test_parsing(tree):
    join_cond = ["Merge Cond", "Hash Cond", "Index Cond"]
    cond_dic = {"Merge Cond": "Merge Join", "Hash Cond": "Hash Join", "Index Cond": "Index-based Join"}
    scans = ["Seq Scan", "Index Scan", "Bitmap Scan", "Index Only Scan"]
    for node in tree.all_nodes():
        res = None
        find_conds = [c for c in join_cond if c in node.data.keys()]
        if len(find_conds) > 0:
            cond = find_conds[0]
            temp_cond = node.data[cond].replace("(", "").replace(")", "")
            for subcond in temp_cond.split(" AND "):
                for subcond1 in subcond.split(" OR "):
                    res = " ".join((cond_dic[cond], "join condition:", parse_cond(subcond), "cost:", str(node.data["Total Cost"])))
        elif node.tag in scans:
            res = " ".join((node.tag, "on table", node.data["Relation Name"], "cost:", str(node.data["Total Cost"])))
        if res:
            print(res)

def compare(tree1, tree2):
    # l1 = [node.tag for node in tree1.all_nodes()]
    # l2 = [node.tag for node in tree2.all_nodes()]
    # l2.remove("Hash")
    # print(l1)
    # print(l2)
    # d = DeepDiff(l1, l2)
    # print(d["iterable_item_removed"])


    print("---------------------------------------------------------------------")
    for node1 in tree2.all_nodes():
        res = []
        for cond in join_cond:
            if cond in node1.data.keys():
                temp_cond = node1.data[cond].replace("(", "").replace(")", "")
                for subcond in temp_cond.split(" AND "):
                    res.append((node1.tag, cond, "condition:", parse_cond(subcond)))
        if len(res) > 0:
            print(node1.tag, cond, "condition:",res)
        # if "Filter" in node1.data.keys():
        #     node2 = [node.tag for node in tree2.all_nodes() if "Filter" in node.data.keys() and
        #              node.data["Filter"].find(node1.data["Filter"]) != -1]
        #     print("Scan approach: QEP",node1.tag, "AQP", node2[0])
        #     print(node1.data["Filter"])
        # if "Hash Cond" in node1.data.keys():
        #     node2 = [node.tag for node in tree2.all_nodes() if "Hash Cond" in node.data.keys() and
        #              node.data["Hash Cond"].find(node1.data["Hash Cond"]) != -1]
        #     print("Hash join approach: QEP:",node1.tag, "AQP:", node2[0])
        #     print(node1.data["Hash Cond"])


def generate_operation_tree(plan):
    tree = Tree()

    queue = []
    node_id = 0
    queue.append((plan, node_id))
    tree.create_node(plan["Node Type"], node_id, data = get_attributes(plan))
    node_id += 1

    while len(queue) > 0:
        node_tuple = queue.pop()
        cur_plan = node_tuple[0]
        cur_plan_id = node_tuple[1]
        if "Plans" in  cur_plan.keys():
            for subplan in cur_plan["Plans"]:
                queue.append((subplan, node_id))
                tag = subplan["Node Type"]
                if subplan["Parent Relationship"] == "SubPlan":
                    tag = tag + "(SubPlan)"
                tree.create_node(tag, node_id, data = get_attributes(subplan), parent = cur_plan_id)
                node_id += 1

    return tree

def get_attributes(plan):
    attributes = {}
    for key, value in plan.items():
        if not key == "Plans" and not key == "Node Type":
            attributes[key] = value
    return attributes

if __name__ == "__main__":
    test_dic = {("p_key = r_key", 2): [("Index-based Join", 10), ("Merge Join", 200), ("Hash Join", 3000)],
                ("part", 10): [("Index Scan", 2), ("Seq Scan", 100)],
                ("region", 3): [("Seq Scan", 100), ("Seq Scan", 100)],
                ("nation", 1): [("Seq Scan", 100), ("Index Scan", 2)],
                ("lineitem", 33): [("Seq Scan", 100)],
                ("sum(p_size)", 5): [("Aggregate", 10)]}
    for item in generate_annotation(test_dic).items():
        print(item)
    print(generate_annotation(test_dic))
    test_dic1 = {}


    start = preprocessing.connect()
    conn = start[0]
    tables = start[1]
    # print(tables)
    query_string = preprocessing.query_asker()
    preprocessing.QEP_Generator(conn, query_string)
    preprocessing.AQP_generator(conn, query_string)
    print("?")
    test = read_json("QEP.json")
    test1 = read_json("AQP4.json")
    test2 = read_json("temp.json")
    print(test)
    tree1 = generate_operation_tree(test[0][0][0]["Plan"])
    tree1.show()
    tree2 = generate_operation_tree(test1[0][0][0]["Plan"])
    tree2.show()
    tree3 = generate_operation_tree(test2[0][0][0]["Plan"])
    tree3.show()
    test_parsing(tree1)
    print("---------------------------------------------")
    test_parsing(tree2)
    # str = "a = b"
    # str = str.split(" = ")
    # print(str)
    # raw = "lineitem.l_shipdate <= '1998:0803000000'::time stamp without time zone"
    # raw = re.sub(r"(\w+\.)?(\w+) (>=|<=|=) ('.*')(::.*)", r'\2 <= \4', raw)
    # print("test",raw)
    # for node in tree2.all_nodes():
    #     print(node.tag, node.identifier)