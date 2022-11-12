from collections import defaultdict
import sqlparse
import re


def parse_sql(raw):
    query = sqlparse.format(raw.strip(), strip_comments=True,
                            reindent=True, keyword_case="upper")
    # print(query)
    query_list = [line.strip() for line in query.split("\n")]

    '''
    sql_to_level_mapping = {
        line: [index_0, index_1, ..., index_n]
    }
    '''
    sql_to_level_mapping = get_sql_to_level_mapping(query_list)
    print_sql_to_level_mapping(sql_to_level_mapping)


def print_sql_to_level_mapping(sql_to_level_mapping):
    for k, v in sql_to_level_mapping.items():
        print(f"Key: {k} | Index: {v}")


def get_sql_to_level_mapping(query_list):
    sql_to_level_mapping = defaultdict(list)
    is_unwanted_line = True
    i = 0
    while i < len(query_list):
        if re.search(r'\(SELECT', query_list[i]):
            modify_key_for_subquery(i, query_list, sql_to_level_mapping)

        has_in_keyword = False
        line_to_skip = 1
        if re.search(r'FROM|WHERE', query_list[i]) or (not is_unwanted_line and not re.search(r'SELECT|ORDER BY', query_list[i])):
            is_unwanted_line = False

            if re.search(r'in \(', query_list[i]):
                has_in_keyword = True
                new_line, local_line_to_skip = modify_line_with_in_keyword(
                    i, query_list)
                query_list[i] = new_line
                line_to_skip = local_line_to_skip
            cleaned_key = remove_unwanted_keywords(
                query_list[i], has_in_keyword)
            sql_to_level_mapping[cleaned_key].append(i)
        else:
            is_unwanted_line = True
        i += line_to_skip

    return sql_to_level_mapping


def modify_key_for_subquery(i, query_list, sql_to_level_mapping):
    cleaned_key = remove_unwanted_keywords(query_list[i-1])
    v = sql_to_level_mapping[cleaned_key]
    del sql_to_level_mapping[cleaned_key]
    sql_to_level_mapping[cleaned_key + " _"] = v


def modify_line_with_in_keyword(start, query_list):
    line = query_list[start]
    line_to_skip = 0
    for i in range(start+1, len(query_list)):
        line += query_list[i]
        line_to_skip += 1
        if re.search(r'\)', query_list[i]):
            break

    stack = []
    temp_line = ""
    for i in line:
        if i == "(":
            stack.append(i)
        elif i == ")" and stack:
            break
        elif stack:
            temp_line += i

    split_line = [i.replace("'", "") for i in temp_line.split(",")]
    new_line = ""
    for i in range(len(split_line)):
        try:
            if i != len(split_line) - 1:
                new_line += int(split_line[i]) + ","
            else:
                new_line += int(split_line[i])
        except:
            format_string = ""
            if len(split_line[i].split(" ")) > 1:
                format_string = '\\"'
            if i != len(split_line) - 1:
                new_line += format_string + split_line[i] + format_string + ","
            else:
                new_line += format_string + split_line[i] + format_string

    return "{" + new_line + "}", line_to_skip + 1


def remove_unwanted_keywords(key, has_in_keyword=False):
    if re.search(r'^(AND|OR|FROM|WHERE|GROUP BY) (.*)', key):
        key = re.sub(r'^(AND|OR|FROM|WHERE|GROUP BY) (.*)', r'\2', key, 1)

    if not has_in_keyword and re.search(r"\(|\)|,|;", key):
        key = re.sub(r"\(|\)|,|;", "", key).strip()

    if re.search(r'^customer|lineitem|nation|orders|part|partsupp|region|supplier', key):
        if len(key.split(" ")) == 2:
            key = key.split(" ")[0]

    key = re.sub(r'(.*\.)(.*) (=|>|<|>=|<=|<>) (.*\.)(.*)', r'\2 \3 \5', key)
    key = re.sub(r'(.*\.)?(.*) (=|>|<|>=|<=|<>) (.*\.)?(.*)(AS.*)',
                 r'\2 \3 \5', key)
    key = re.sub(r'(.*JOIN.*ON.*) (.*) = (.*)', r'\2 = \3', key)

    return key
