from __future__ import annotations

from contextlib import suppress
from enum import Enum
from pathlib import Path
from sys import maxsize
from typing import Optional, Mapping, Any, Union, Generator, List

import lmdb
import pynndb
import pynndb.decorators


Doc = pynndb.Doc
Duplicate = pynndb.DuplicateKey
Index = pynndb.Index
Node = Doc
Transaction = lmdb.Transaction


class What(Enum):
    Node = "node"
    Edge = "edge"


class IndexBy(Enum):
    u_v = "by_u_v"
    u = "by_u"
    v = "by_v"


class Edge(Doc):
    def __init__(self, u: Node, v: Node, doc: Mapping[str, Any]) -> None:
        d = dict(doc)
        if u is not None:
            d["u"] = u.oid
        if v is not None:
            d["v"] = v.oid
        super().__init__(doc=d)


class Database:
    def __init__(self, path: Union[Path, str], config: Optional[Mapping[str, Any]] = None):
        self._db = pynndb.Database()
        if config is None:
            config = {}
        if "subdir" not in config:
            config["subdir"] = False
        self._db.configure(config)
        self._db.open(str(path))
        with self._db.write_transaction as txn:
            self._node_table = self._db.table(What.Node.value, txn=txn)
            self._edge_table = self._db.table(What.Edge.value, txn=txn)
            self._edge_table.ensure(IndexBy.u_v.value, "!{u}|{v}", duplicates=True, txn=txn)
            self._edge_table.ensure(IndexBy.u.value, "{u}", duplicates=True, txn=txn)
            self._edge_table.ensure(IndexBy.v.value, "{v}", duplicates=True, txn=txn)

    def ensure_index(
            self,
            what: What,
            name: str,
            func: Optional[str] = None,
            duplicates: bool = False,
            force: bool = False,
            txn: Optional[Transaction] = None,
    ) -> pynndb.Index:
        return self._db[what.value].ensure(index_name=name, func=func, duplicates=duplicates, force=force, txn=txn)

    @property
    def read_transaction(self) -> Transaction:
        return self._db.read_transaction

    @property
    def write_transaction(self) -> Transaction:
        return self._db.write_transaction

    def add_node(self, node: Node, txn: Optional[Transaction] = None) -> None:
        self._node_table.append(node, txn=txn)

    def update_node(self, node: Node, txn: Optional[Transaction] = None) -> None:
        self._node_table.save(node, txn=txn)

    def add_edge(self, edge: Edge, txn: Optional[Transaction] = None) -> None:
        self._edge_table.append(edge, txn=txn)

    def update_edge(self, edge: Edge, txn: Optional[Transaction] = None) -> None:
        self._edge_table.save(edge, txn=txn)

    def range(
            self,
            what: What,
            lower: Optional[Doc]=None,
            upper: Optional[Doc]=None,
            index_name: Optional[str]=None,
            oids_only: bool=False,
            inclusive: bool=True,
            txn: Optional[Transaction]=None) -> Generator[Doc, None, None]:
        if index_name is None:
            indexes_names = self.get_indexes(what=what, doc=lower)
        else:
            indexes_names = [index_name]
        result_oids = None
        for index_name in indexes_names:
            oids = set()
            for cursor in self._db[what.value].range(
                index_name=index_name,
                lower=lower,
                upper=upper,
                keyonly=True,
                inclusive=inclusive,
                txn=txn,
            ):
                oids.add(cursor.val)
            if result_oids is None:
                result_oids = oids
            else:
                result_oids.intersection_update(oids)
        if result_oids is None:
            result_oids = []
        if oids_only:
            yield from result_oids
        else:
            for oid in result_oids:
                yield self._db[what.value].get(oid, txn=txn)

    def seek(
            self,
            what: What,
            doc: Doc,
            limit: int = maxsize,
            index_name: Optional[str] = None,
            oids_only: bool = False,
            txn: Optional[Transaction] = None
    ):
        if index_name is None:
            indexes_names = self.get_indexes(what=what, doc=doc)
        else:
            indexes_names = [index_name]
        result_oids = None
        for index_name in indexes_names:
            oids = set()
            for cursor in self._db[what.value].seek(
                    index_name=index_name,
                    doc=doc,
                    limit=limit,
                    keyonly=True,
                    txn=txn,
            ):
                oids.add(cursor.val)
            if result_oids is None:
                result_oids = oids
            else:
                result_oids.intersection_update(oids)
        if result_oids is None:
            result_oids = []
        if oids_only:
            yield from result_oids
        else:
            for oid in result_oids:
                yield self._db[what.value].get(oid, txn=txn)
        # if index_name is None:
        #     index_name = self.get_indexes(what=what, doc=doc)
        # yield from self._db[what.value].seek(index_name=index_name, doc=doc, limit=limit, txn=txn)

    def seek_one(self, what: What, doc: Doc, index_name: Optional[str] = None, txn: Optional[Transaction] = None):
        if index_name is None:
            index_name = self.get_indexes(what=what, doc=doc)[0]
        return self._db[what.value].seek_one(index_name=index_name, doc=doc, txn=txn)

    def get_indexes(self, what: What, doc: Doc) -> List[str]:
        raise NotImplementedError("LegDB.get_index_name should be overridden in subclasses")
