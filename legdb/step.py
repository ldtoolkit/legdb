from __future__ import annotations

from abc import ABC
from itertools import islice
from typing import Type, Any, Optional, Dict, Callable, Set, List, Collection, FrozenSet

import lmdb
import pynndb

from legdb import Entity, Database, Node


class Step:
    def __init__(self):
        self.output_oids = set()

    def input(self, args: Collection[Any]) -> None:
        pass

    def output(self) -> List[Entity]:
        pass

    def process(self, entity: Entity) -> bool:
        if entity.oid in self.output_oids:
            return False
        else:
            self.output_oids.add(entity.oid)
            entity.disconnect()
            return True


class SourceStep(Step):
    def __init__(self, what: Type[Entity]) -> None:
        super().__init__()
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


class EdgeInStep(Step):
    def __init__(self, **kwargs) -> None:
        super().__init__()
        self.attrs = kwargs

    def __repr__(self) -> str:
        attrs_str = _attrs_str(self.attrs)
        return f"edge_in({attrs_str})"


class EdgeOutStep(Step):
    def __init__(self, **kwargs) -> None:
        super().__init__()
        self.attrs = kwargs

    def __repr__(self) -> str:
        attrs_str = _attrs_str(self.attrs)
        return f"edge_out({attrs_str})"


class EdgeAllStep(Step):
    def __init__(self, **kwargs) -> None:
        super().__init__()
        self.attrs = kwargs

    def __repr__(self) -> str:
        attrs_str = _attrs_str(self.attrs)
        return f"edge_all({attrs_str})"


class PynndbStep(Step):
    def __init__(self, database: Database, page_size: int, txn: lmdb.Transaction) -> None:
        super().__init__()
        self.database = database
        self.page_size = page_size
        self.txn = txn

    def __getstate__(self):
        state = self.__dict__.copy()
        del state["txn"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.txn = self.database.read_transaction

    def __iter__(self):
        raise NotImplementedError()


class PynndbFilterStepBase(PynndbStep, ABC):
    def __init__(
            self,
            database: Database,
            what: Type[Entity],
            page_size: int,
            txn: lmdb.Transaction,
            attrs: Optional[Dict[str, Any]] = None,
            filter_func: Optional[Callable[[pynndb.Doc], bool]] = None,
    ) -> None:
        super().__init__(database=database, page_size=page_size, txn=txn)
        self.attrs = attrs
        self.filter_func = filter_func
        self.what = what
        self.table: pynndb.Table = self.database._db.table(table_name=self.what.table_name, txn=self.txn)

        self.attrs_to_check = {}
        self.index_names = {}

    def __getstate__(self):
        state = super().__getstate__()
        del state["table"]
        return state

    def __setstate__(self, state):
        super().__setstate__(state)
        self.table = self.database._db.table(table_name=self.what.table_name, txn=self.txn)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, PynndbFilterStepBase):
            return NotImplemented
        return (self.attrs == other.attrs
                and self.database == other.database
                and self.txn == other.txn
                and self.what == other.what)

    def count(self, index_name: str, doc: pynndb.Doc) -> int:
        filter_results = list(self.table.filter(index_name=index_name, lower=doc, page_size=1, txn=self.txn))
        return filter_results[0].count if filter_results else 0

    def create_filter_func(self, doc: pynndb.Doc, attr_names: Set[str]) -> Callable[[pynndb.Doc], bool]:
        def filter_func(result_doc: pynndb.Doc):
            for attr in self.database._attrs_to_check_by_attr_names[attr_names]:
                if "[" in attr and "]" in attr:
                    attr0, attr1 = attr.replace("[", " ").replace("]", " ").split()
                    if result_doc[attr0][attr1] != doc[attr0][attr1]:
                        return False
                else:
                    if result_doc[attr] != doc[attr]:
                        return False
            return True

        return filter_func

    @staticmethod
    def get_attr_names(doc: pynndb.Doc) -> FrozenSet[str]:
        result = []
        for key0, value in doc.items():
            if isinstance(value, dict):
                for key1 in value:
                    result.append(f"{key0}[{key1}]")
            else:
                result.append(f"{key0}")
        return frozenset(result)

    def select_index_and_filter_func(self, doc: pynndb.Doc):
        attr_names = self.get_attr_names(doc)
        attrs_to_check = self.database._attrs_to_check_by_attr_names.get(attr_names)
        if attrs_to_check is None:
            relevant_indexes = {}
            for index_name in self.table.indexes(txn=self.txn):
                if self.database._index_attrs[(self.what.table_name, index_name)].issubset(attr_names):
                    relevant_indexes[index_name] = self.count(index_name=index_name, doc=doc)

            if relevant_indexes:
                self.database._index_names_by_attr_names[attr_names] = min(relevant_indexes, key=relevant_indexes.get)
                self.database._attrs_to_check_by_attr_names[attr_names] = (
                        attr_names - self.database._index_attrs[
                    (self.what.table_name, self.database._index_names_by_attr_names[attr_names])
                ])
            else:
                self.database._index_names_by_attr_names[attr_names] = None
                self.database._attrs_to_check_by_attr_names[attr_names] = attr_names

        index_name = self.database._index_names_by_attr_names[attr_names]
        filter_func = (self.create_filter_func(doc, attr_names)
                       if self.database._attrs_to_check_by_attr_names[attr_names] else
                       None)
        return index_name, filter_func


class PynndbFilterStep(PynndbFilterStepBase):
    def __init__(
            self,
            database: Database,
            what: Type[Entity],
            page_size: int,
            txn: lmdb.Transaction,
            attrs: Optional[Dict[str, Any]] = None,
            filter_func: Optional[Callable[[pynndb.Doc], bool]] = None,
    ):
        super().__init__(
            database=database,
            what=what,
            attrs=attrs,
            page_size=page_size,
            txn=txn,
            filter_func=filter_func,
        )
        self.doc_attrs = [self.attrs] if self.attrs else [None]
        self.iter = iter(self)

    def __getstate__(self):
        state = super().__getstate__()
        del state["iter"]
        return state

    def __setstate__(self, state):
        super().__setstate__(state)
        self.reset_iter()

    def input_attrs(self, **kwargs) -> None:
        self.doc_attrs.append({**kwargs, **self.attrs})

    def __iter__(self):
        while self.doc_attrs:
            doc_attrs = self.doc_attrs.pop(0)
            if doc_attrs is not None:
                doc = pynndb.Doc(doc_attrs)
                index_name, filter_func = self.select_index_and_filter_func(doc)
            else:
                doc = None
                index_name = None
                filter_func = None
            filter_result = self.table.filter(
                index_name=index_name,
                lower=doc,
                upper=doc,
                expression=filter_func,
                txn=self.txn,
            )
            result = (self.what.from_doc(doc=result.doc, db=self.database, txn=self.txn) for result in filter_result)
            yield from (edge for edge in result if self.process(edge))

    def reset_iter(self):
        self.iter = iter(self)

    def output(self) -> List[Entity]:
        return list(islice(self.iter, self.page_size))


class PynndbEdgeBaseStep(PynndbFilterStep):
    def __init__(
            self,
            database: Database,
            what: Type[Entity],
            attrs: Dict[str, Any],
            page_size: int,
            txn: lmdb.Transaction,
            filter_func: Optional[Callable[[pynndb.Doc], bool]] = None,
    ):
        super().__init__(
            database=database,
            what=what,
            attrs=None,
            page_size=page_size,
            txn=txn,
            filter_func=filter_func,
        )
        self.attrs = attrs

    def input_node(self, node: Node) -> None:
        raise NotImplementedError()

    def input(self, args: Collection[Any]) -> None:
        for arg in args:
            self.input_node(node=arg)


class PynndbEdgeInStep(PynndbEdgeBaseStep):
    def input_node(self, node: Node) -> None:
        self.input_attrs(end_id=node.oid)


class PynndbEdgeOutStep(PynndbEdgeBaseStep):
    def input_node(self, node: Node) -> None:
        self.input_attrs(start_id=node.oid)


class PynndbEdgeAllStep(PynndbEdgeBaseStep):
    def input_node(self, node: Node) -> None:
        self.input_attrs(start_id=node.oid)
        self.input_attrs(end_id=node.oid)


class PynndbUnionStep(PynndbStep):
    def __init__(
            self,
            database: Database,
            page_size: int,
            steps: List[PynndbStep],
            txn: lmdb.Transaction,
    ):
        super().__init__(database=database, page_size=page_size, txn=txn)
        self.steps = steps
        self.steps_iter = [iter(step) for step in self.steps]

    def output(self) -> List[Entity]:
        result = None
        while not result and self.steps_iter:
            step_iter = self.steps_iter[0]
            result = [entity for entity in islice(step_iter, self.page_size) if self.process(entity)]
            if not result:
                self.steps_iter.pop(0)
        return result
