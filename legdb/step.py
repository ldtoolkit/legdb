from __future__ import annotations

from typing import Type, Any, Optional, Dict, Callable, Set

import lmdb
import pynndb

from legdb import Entity, Database, Node


class Step:
    def input(self, arg: Any) -> None:
        pass


class SourceStep(Step):
    def __init__(self, what: Type[Entity]) -> None:
        self.what = what

    def __repr__(self) -> str:
        return self.what.table_name


def _attrs_str(attrs: Dict[str, Any]) -> str:
    return ", ".join(f"{key}={value!r}" for key, value in attrs.items())


class HasStep(Step):
    def __init__(self, **kwargs) -> None:
        super().__init__()
        self.attrs = kwargs

    def __repr__(self) -> str:
        attrs_str = _attrs_str(self.attrs)
        return f"has({attrs_str})"


class InEStep(Step):
    def __init__(self, **kwargs) -> None:
        super().__init__()
        self.attrs = kwargs

    def __repr__(self) -> str:
        attrs_str = _attrs_str(self.attrs)
        return f"inE({attrs_str})"


class OutEStep(Step):
    def __init__(self, **kwargs) -> None:
        super().__init__()
        self.attrs = kwargs

    def __repr__(self) -> str:
        attrs_str = _attrs_str(self.attrs)
        return f"outE({attrs_str})"


class BothEStep(Step):
    def __init__(self, **kwargs) -> None:
        super().__init__()
        self.attrs = kwargs

    def __repr__(self) -> str:
        attrs_str = _attrs_str(self.attrs)
        return f"bothE({attrs_str})"


class PynndbStep(Step):
    def __init__(self, database: Database, txn: lmdb.Transaction) -> None:
        self.database = database
        self.txn = txn


class PynndbFilterStepBase(PynndbStep):
    def __init__(
            self,
            database: Database,
            what: Type[Entity],
            txn: lmdb.Transaction,
            attrs: Optional[Dict[str, Any]] = None,
            filter_func: Optional[Callable[[pynndb.Doc], bool]] = None,
    ) -> None:
        super().__init__(database=database, txn=txn)
        self.attrs = attrs
        self.doc = pynndb.Doc(self.attrs) if self.attrs is not None else None
        self.filter_func = filter_func
        self.table: pynndb.Table = database._db.table(table_name=what.table_name, txn=txn)
        self.what = what

        self.attrs_to_check = {}
        self.index_names = {}

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, PynndbFilterStepBase):
            return NotImplemented
        return (self.attrs == other.attrs
                and self.database == other.database
                and self.txn == other.txn
                and self.what == other.what)

    def count(self, index_name: str, doc: pynndb.Doc) -> int:
        filter_result = next(self.table.filter(index_name=index_name, lower=doc, page_size=1))
        return filter_result.count

    def create_filter_func(self, doc: pynndb.Doc, attr_names: Set[str]) -> Callable[[pynndb.Doc], bool]:
        def filter_func(result_doc: pynndb.Doc):
            for attr in self.attrs_to_check[attr_names]:
                if result_doc[attr] != doc[attr]:
                    return False
            return True

        return filter_func

    def select_index_and_filter_func(self, doc: pynndb.Doc):
        attr_names = frozenset(doc.keys())
        attrs_to_check = self.attrs_to_check.get(attr_names)
        if attrs_to_check is None:
            relevant_indexes = {}
            for index_name in self.table.indexes(txn=self.txn):
                if self.database._index_attrs[(self.what.table_name, index_name)].issubset(attr_names):
                    relevant_indexes[index_name] = self.count(index_name=index_name, doc=doc)

            if relevant_indexes:
                self.index_names[attr_names] = min(relevant_indexes, key=relevant_indexes.get)
                self.attrs_to_check[attr_names] = (
                        attr_names - self.database._index_attrs[(self.what.table_name, self.index_names[attr_names])]
                )
            else:
                self.index_names[attr_names] = None
                self.attrs_to_check[attr_names] = attr_names

        index_name = self.index_names[attr_names]
        filter_func = self.create_filter_func(doc, attr_names) if self.attrs_to_check else None
        return index_name, filter_func


class PynndbFilterStep(PynndbFilterStepBase):
    def __init__(
            self,
            database: Database,
            what: Type[Entity],
            txn: lmdb.Transaction,
            attrs: Optional[Dict[str, Any]] = None,
            filter_func: Optional[Callable[[pynndb.Doc], bool]] = None,
    ):
        super().__init__(
            database=database,
            what=what,
            attrs=attrs,
            txn=txn,
            filter_func=filter_func,
        )
        self.docs = [self.doc] if self.doc is not None else []

    def input_attrs(self, **kwargs) -> None:
        self.docs.append(pynndb.Doc({**kwargs, **self.attrs}))

    def __iter__(self):
        for doc in self.docs:
            index_name, filter_func = self.select_index_and_filter_func(doc)
            filter_result = self.table.filter(
                index_name=index_name,
                lower=doc,
                upper=doc,
                expression=filter_func,
                txn=self.txn,
            )
            yield from (self.what.from_doc(doc=result.doc, db=self.database, txn=self.txn) for result in filter_result)


class PynndbEdgeBaseStep(PynndbFilterStep):
    def __init__(
            self,
            database: Database,
            what: Type[Entity],
            attrs: Dict[str, Any],
            txn: lmdb.Transaction,
            filter_func: Optional[Callable[[pynndb.Doc], bool]] = None,
    ):
        super().__init__(
            database=database,
            what=what,
            attrs=None,
            txn=txn,
            filter_func=filter_func,
        )
        self.attrs = attrs

    def input_node(self, node: Node) -> None:
        raise NotImplementedError()

    def input(self, arg: Any) -> None:
        self.input_node(node=arg)


class PynndbInEStep(PynndbEdgeBaseStep):
    def input_node(self, node: Node) -> None:
        self.input_attrs(end_id=node.oid)


class PynndbOutEStep(PynndbEdgeBaseStep):
    def input_node(self, node: Node) -> None:
        self.input_attrs(start_id=node.oid)


class PynndbBothEStep(PynndbEdgeBaseStep):
    def input_node(self, node: Node) -> None:
        self.input_attrs(start_id=node.oid)
        self.input_attrs(end_id=node.oid)
