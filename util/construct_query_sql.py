# -*- coding: utf-8 -*-

import json

# // (timestamp > 2024-07-01 09:41:47 and timestamp <= 2024-07-01 10:41:47) and (flow in ("UP", "DOWN") or id = "SIMU-G4G7OZ3TUA08")

example_data = '''
{
    "tableName": "doris_msg_store",
    "filter": {
        "type": "and",
        "value": [
            {
                "type": "or",
                "value": [
                    {
                        "type": "illogic",
                        "value": {
                            "columnName": "timestamp",
                            "condition": {
                                "gt": "2024-07-01 09:41:47",
                                "le": "2024-07-01 10:41:47"
                            }
                        }
                    },
                    {
                        "type": "illogic",
                        "value": {
                            "columnName": "id",
                            "condition": {
                                "eq": "SIMU-G4G7OZ3TUA08"
                            }
                        }
                    }
                ]
            },
            {
                "type": "or",
                "value": [
                    {
                        "type": "illogic",
                        "value": {
                            "columnName": "flow",
                            "condition": {
                                "in": [
                                    "UP",
                                    "DOWN"
                                ]
                            }
                        }
                    },
                    {
                        "type": "illogic",
                        "value": {
                            "columnName": "id",
                            "condition": {
                                "eq": "SIMU-G4G7OZ3TUA08"
                            }
                        }
                    }
                ]
            }
        ]
    }
}
'''
example_data2 = '''
{
	"tableName":"sample_tbl",
	"filter": {
		"type": "or",
		"value": [
			{
				"type": "illogic",
				"value":{
					"columnName":"city",
					"condition": {
						"eq": "beijing"
					}
				}
			}, 
			{
				"type": "illogic",
				"value":{
					"columnName":"age",
					"condition": {
						"le": 30
					}
				}
			}
		]
	}
}
'''


def parse(req_data):
    """
    解析请求参数，生成查询语句
    Args:
        data: json format data

    Returns:
        query doris sql sentence
    """
    # req_data = json.loads(data)
    table_name = req_data['tableName']
    filter_json = req_data['filter']
    where_list = []
    format_condition(filter_json, where_list)
    if len(where_list) > 0:
        condition_sentence = f'where {where_list[0]}'
    else:
        condition_sentence = ''
    query_sentence = f'select * from {table_name} {condition_sentence}'
    print(query_sentence)
    return query_sentence


op_dict = {'eq': '=', 'lt': '<', 'le': '<=', 'gt': '>', 'ge': '>=', 'nt': '!='}


def add_blank(*args):
    return ' '.join(args)


class FilterTypeError(Exception):
    pass


def format_condition(filter_dict: dict, where_list: list):
    """
    根据参数构建条件语句
    Args:
        filter_dict:
            过滤条件对应参数的字典
        where_list:
            记录条件语句的列表

    Returns:
        查询条件的列表

    """
    filter_type = filter_dict['type']
    value = filter_dict['value']
    if filter_type not in ['illogic', 'and', 'or']:
        raise FilterTypeError(f"输入过滤类型不正确: type = {filter_type}，请检查！")

    if filter_type == 'illogic':
        # 存储非逻辑操作的条件
        tmp_list = []
        column_name = value['columnName']
        condition = value['condition']
        for k, v in condition.items():
            if k != 'in':
                revise_v = v
                if isinstance(v, str):
                    revise_v = "'" + v + "'"
                tmp_list.append(add_blank(column_name, op_dict[k], str(revise_v)))
            else:
                if isinstance(v[0], str):
                    tmp_list.append(add_blank(column_name, "in", "('" + "','".join(v) + "')"))
                else:
                    tmp_list.append(add_blank(column_name, "in", "(" + ",".join(v) + ")"))
        if len(tmp_list) == 1:
            where_list.append(tmp_list[0])
        else:
            where_list.append(" and ".join(tmp_list))

    if filter_type == "and" or filter_type == "or":
        for sub_filter in value:
            format_condition(sub_filter, where_list)
        # 暂存逻辑操作对应的条件语句
        tmp_list = []
        for i in range(len(value)):
            # 通过pop达到只取当前逻辑操作的条件
            tmp_list.append(where_list.pop())
        logic_condition = "(" + f") {filter_type} (".join(tmp_list) + ")"
        where_list.append(logic_condition)


if __name__ == "__main__":
    sql = parse(json.loads(example_data2))
    from tasks.doris_query import sync_query

    print(sync_query(sql))
