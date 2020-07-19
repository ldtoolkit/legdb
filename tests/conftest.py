import os
import string
from dataclasses import dataclass
from itertools import product
from pathlib import Path
from typing import Optional, Union, Mapping, Any

from pytest import fixture

import legdb
from legdb import DbOpenMode


@dataclass
class Node(legdb.Node):
    c: Optional[str] = None
    ord_c_mod_2: Optional[int] = None
    ord_c_mod_3: Optional[int] = None
    ord_c_mod_4: Optional[int] = None


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
        self.ensure_index(what=Node, name="by_c", attrs=["c"], func="{c}", duplicates=False)
        self.ensure_index(
            what=Node,
            name="by_ord_c_mod_2",
            attrs=["ord_c_mod_2"],
            func="{ord_c_mod_2}",
            duplicates=True,
        )
        self.ensure_index(
            what=Node,
            name="by_ord_c_mod_3",
            attrs=["ord_c_mod_3"],
            func="{ord_c_mod_3}",
            duplicates=True,
        )
        self.ensure_index(what=Edge, name="by_w", attrs=["w"], func="{w}", duplicates=True)


@fixture
def database(tmp_path):
    database = Database(tmp_path / "test.db")
    nodes = []
    for c in string.ascii_lowercase:
        node = database.save(Node(c=c, ord_c_mod_2=ord(c) % 2, ord_c_mod_3=ord(c) % 3, ord_c_mod_4=ord(c) % 4))
        nodes.append(node)
    for start_node, end_node in product(nodes, nodes):
        w = ord(end_node.c) - ord(start_node.c)
        _ = database.save(Edge(start=start_node, end=end_node, w=w))
    return database


@fixture(params=[1, 2, 10, 4096, 10000])
def page_size(request):
    return request.param
