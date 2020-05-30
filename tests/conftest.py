import os
import string
from dataclasses import dataclass
from itertools import product
from pathlib import Path
from typing import Optional, List, Union, Mapping, Any

from pytest import fixture

import legdb
from legdb import Entity, DbOpenMode


@dataclass
class Node(legdb.Node):
    c: Optional[str] = None
    ord_c_mod_2: Optional[int] = None


@dataclass
class Edge(legdb.Edge):
    w: Optional[float] = None

    _node_class = Node


class Database(legdb.Database):
    def __init__(
            self,
            path: Union[Path, str],
            db_open_mode: DbOpenMode = DbOpenMode.READ_WRITE,
            config: Optional[Mapping[str, Any]] = None,
            n_jobs: int = len(os.sched_getaffinity(0)),
    ):
        super().__init__(path=path, db_open_mode=db_open_mode, config=config, n_jobs=n_jobs)
        self.ensure_index(what=Node, name="by_c", func="{c}", duplicates=False)
        self.ensure_index(what=Node, name="by_ord_c_mod_2", func="{ord_c_mod_2}", duplicates=True)
        self.ensure_index(what=Edge, name="by_w", func="{w}", duplicates=True)

    def get_indexes(self, entity: Entity) -> List[str]:
        result = []
        if isinstance(entity, Node):
            if entity.c is not None:
                result.append("by_c")
            if entity.ord_c_mod_2 is not None:
                result.append("by_ord_c_mod_2")
        elif isinstance(entity, Edge):
            if entity.start_id is not None and entity.end_id is not None:
                result.append(legdb.IndexBy.start_id_end_id.value)
            else:
                if entity.start_id is not None:
                    result.append(legdb.IndexBy.start_id.value)
                elif entity.end_id is not None:
                    result.append(legdb.IndexBy.end_id.value)
                if entity.w is not None:
                    result.append("by_w")
        return result


@fixture
def database(tmp_path):
    database = Database(tmp_path / "test.db")
    nodes = []
    for c in string.ascii_lowercase:
        node = database.save(Node(c=c, ord_c_mod_2=ord(c) % 2))
        nodes.append(node)
    for start_node, end_node in product(nodes, nodes):
        w = ord(end_node.c) - ord(start_node.c)
        _ = database.save(Edge(start=start_node, end=end_node, w=w))
    return database
