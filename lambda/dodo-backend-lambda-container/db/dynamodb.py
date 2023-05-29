from typing import Dict


def put_item(Item: Dict, table):
    return table.put_item(Item=Item)
