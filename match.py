from sql_parser import parse_sql
from difflib import SequenceMatcher
import re

qep_summary = {
    "orders": [
        [
            "Seq Scan",
            67614.45,
            5,
            "Hash Join"
        ]
    ],
    "customer": [
        [
            "Seq Scan",
            67614.45,
            7,
            "Hash Join"
        ]
    ],
    "n_name = 'BRAZIL'": [
        [
            "Hash Join",
            67614.45,
            2,
            "Sort"
        ]
    ],
    "n_name = 'CANADA'": [
        [
            "Hash Join",
            67614.45,
            2,
            "Sort"
        ]
    ],
    "nation n2": [
        [
            "Seq Scan",
            67614.45,
            8,
            "Hash"
        ]
    ],
    "c_nationkey = n_nationkey": [
        [
            "Hash Join",
            67614.45,
            6,
            "Hash"
        ]
    ],
    "o_custkey = c_custkey": [
        [
            "Hash Join",
            67614.45,
            4,
            "Nested Loop"
        ]
    ],
    "l_orderkey = o_orderkey": [
        [
            "Index Join",
            67614.45,
            4,
            "Nested Loop"
        ]
    ],
    "supplier": [
        [
            "Seq Scan",
            67614.45,
            5,
            "Hash Join"
        ]
    ],
    "nation n1": [
        [
            "Seq Scan",
            67614.45,
            6,
            "Hash"
        ]
    ],
    "s_nationkey = n_nationkey": [
        [
            "Hash Join",
            67614.45,
            4,
            "Hash"
        ]
    ],
    "l_suppkey = s_suppkey": [
        [
            "Hash Join",
            67614.45,
            2,
            "Sort"
        ]
    ]
}
f = open("Queries&Json\\q7.sql", "r")
sql = f.read()
f.close()


def longest_common_substring(candidates, string, threshold):
    if len(candidates) == 0:
        return None
    longest_match_size = threshold
    chosen = None
    common_substring = None
    for e, e_refined in candidates:
        print(e_refined, string)
        match = SequenceMatcher(None, e_refined, string).find_longest_match()
        if match.size == longest_match_size:
            print(string)
            print(e_refined)
            print(chosen, common_substring)
            print(f"SAME SUBSTRING LEN {match.size}\n")
        if match.size > longest_match_size:
            longest_match_size = match.size
            chosen = e
            common_substring = string[match.b:match.b+match.size]

    return chosen

def add_to_res(qep_summary, sql_summary, sql_key, qep_key):
    if len(qep_summary[qep_key]) > 1:
        print(f"There are duplicates, {qep_key}")
    else:
        res[(sql_key, tuple(sql_summary[sql_key]))] = qep_summary[qep_key][0]

if __name__ == "__main__":
    sql_summary = parse_sql(sql)

    res = {}
    missing_keys = []
    qep_list = list(qep_summary)
    print(qep_list)
    for key in sql_summary.keys():
        if key in qep_list:
            add_to_res(qep_summary, sql_summary, key, key)

        elif " date " in key:
            # edge case: date
            conds_with_date = []
            for e in qep_list:
                if re.match("(.*)(>=|<=|=|<|>|<>)(.*)(([0-9]){4})-(([0-9]){2})-(([0-9]){2})(.*)", e):
                    index = re.search("(>=|<=|=|<|>|<>)", e).end()
                    e_refined = e[:index+1] + "date " + e[index+1:]
                    conds_with_date.append((e, e_refined))
            # compute threshold
            threshold = len(re.split("(>=|<=|=|<|>|<>)", key)[0].strip())
            replacement_key = longest_common_substring(conds_with_date, key, threshold)
            if replacement_key != None:
                add_to_res(qep_summary, sql_summary, key, replacement_key)
            else:
                missing_keys.append(key)
        else:
            missing_keys.append(key)

    keys_in_res = [e[0] for e in list(res)]
    missing_keys_cleaned = []
    for key in missing_keys:
        if "=" in key:
            args = key.split("=")
            for i in range(len(args)):
                args[i] = args[i].strip()
            s = f"{args[1]} = {args[0]}"
            if s in keys_in_res:
                continue
            else:
                missing_keys_cleaned.append(key)
        else:
            missing_keys_cleaned.append(key)

    for key in missing_keys_cleaned:
        print(key)
    print(f"Number of keys missing, sql -> json = {len(missing_keys_cleaned)}")

    for k, v in res.items():
         print(f"{k}:{v}")

