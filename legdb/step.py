from __future__ import annotations

from abc import ABC
from itertools import islice, chain
from typing import Type, Any, Optional, Dict, Callable, Set, List, Collection

import lmdb
import pynndb
from joblib import Parallel, delayed
from more_itertools.more import divide

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


class ParallelStep(Step):
    def __init__(self, steps: Collection[PynndbStep], n_jobs: int, page_size: int) -> None:
        super().__init__()
        self.n_jobs = n_jobs
        self.steps = list(steps)
        self.iter = iter(self)
        self.page_size = page_size

    def input(self, args: Collection[Any]) -> None:
        divided_args = divide(len(self.steps), args)
        for step, args in zip(self.steps, divided_args):
            args = list(args)
            step.input(args)

    def reset_iter(self):
        self.iter = iter(self)

    def __iter__(self):
        result = chain(*Parallel(n_jobs=self.n_jobs)(delayed(step.output)() for step in self.steps))
        yield from (entity for entity in result if self.process(entity))

    def output(self) -> List[Entity]:
        return list(islice(self.iter, self.page_size))


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
        self.doc_attrs = [self.attrs] if self.attrs else []
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
            doc = pynndb.Doc(self.doc_attrs.pop(0))
            index_name, filter_func = self.select_index_and_filter_func(doc)
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
