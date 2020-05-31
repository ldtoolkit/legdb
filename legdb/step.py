from __future__ import annotations

from typing import Type, Any, Optional, Dict, Callable, Set

import lmdb
import pynndb

from legdb import Entity, Database, Node, Edge


class StepBuilder:
    def __init__(
            self,
            database: Optional[Database] = None,
            node_cls: Type[Entity] = Node,
            edge_cls: Type[Entity] = Edge,
            txn: Optional[lmdb.Transaction] = None,
    ) -> None:
        self._compiled_steps = []
        self._database = database
        self._edge_cls = edge_cls
        self._is_compiled = False
        self._node_cls = node_cls
        self._steps = []
        self._txn = txn

    def source(self, what: Type[Entity]) -> StepBuilder:
        if self._steps:
            raise ValueError("Step 'source' should be the first.")

        self._steps.append(SourceStep(what=what))
        return self

    def has(self, **kwargs) -> StepBuilder:
        self._steps.append(HasStep(**kwargs))
        return self

    def out_e(self, **kwargs) -> StepBuilder:
        self._steps.append(OutEStep(**kwargs))
        return self

    def __repr__(self) -> str:
        return ".".join(repr(step) for step in self._steps)

    def _compile(self) -> None:
        if self._is_compiled:
            return
        self._compiled_steps = []
        step = self._steps.pop(0)
        if isinstance(step, SourceStep):
            step = PynndbFilterStep(database=self._database, what=step.what, attrs={}, txn=self._txn, is_root=True)
        else:
            raise ValueError("Step 'source' should be the first.")
        self._compiled_steps.append(step)
        while self._steps:
            next_step = self._steps.pop(0)
            if isinstance(step, PynndbFilterStep) and isinstance(next_step, HasStep):
                attrs = {**step.attrs, **next_step.attrs}
                step = PynndbFilterStep(
                    database=self._database,
                    what=step.what,
                    attrs=attrs,
                    txn=self._txn,
                    is_root=True,
                )
                self._compiled_steps.pop()
                self._compiled_steps.append(step)
            elif isinstance(step, PynndbFilterStep) and isinstance(next_step, OutEStep):
                step = PynndbOutEStep(
                    database=self._database,
                    what=self._edge_cls,
                    attrs=next_step.attrs,
                    txn=self._txn,
                )
                self._compiled_steps.append(step)
                step = next_step
            else:
                self._compiled_steps.append(step)
                step = next_step
        self._is_compiled = True

    def __iter__(self):
        self._compile()
        last = len(self._compiled_steps) - 1
        step_iterators = [iter(step) for step in self._compiled_steps]
        while True:
            entity = None
            try:
                for i, (step, step_iterator) in enumerate(zip(self._compiled_steps, step_iterators)):
                    if entity is not None:
                        step.input(entity)
                    entity = next(step_iterator)
                    if i == last:
                        yield entity
            except StopIteration:
                break


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


class OutEStep(Step):
    def __init__(self, **kwargs) -> None:
        super().__init__()
        self.attrs = kwargs

    def __repr__(self) -> str:
        attrs_str = _attrs_str(self.attrs)
        return f"outE({attrs_str})"


class PynndbStep(Step):
    def __init__(self, database: Database, txn: lmdb.Transaction, is_root: bool = False) -> None:
        self.database = database
        self.is_root = is_root
        self.txn = txn


class PynndbFilterStepBase(PynndbStep):
    def __init__(
            self,
            database: Database,
            what: Type[Entity],
            attrs: Dict[str, Any],
            txn: lmdb.Transaction,
            filter_func: Optional[Callable[[pynndb.Doc], bool]] = None,
            attrs_to_check: Optional[Set[str]] = None,
            index_name: Optional[str] = None,
            is_root: bool = False,
    ) -> None:
        super().__init__(database=database, txn=txn, is_root=is_root)
        self.attrs = attrs
        self.attrs_to_check = attrs_to_check
        self.doc = pynndb.Doc(self.attrs)
        self.filter_func = filter_func
        self.index_name = index_name
        self.table: pynndb.Table = database._db.table(table_name=what.table_name, txn=txn)
        self.what = what

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, PynndbFilterStepBase):
            return NotImplemented
        return (self.attrs == other.attrs
                and self.attrs_to_check == other.attrs_to_check
                and self.index_name == other.index_name
                and self.database == other.database
                and self.txn == other.txn
                and self.what == other.what)

    def count(self, index_name: str, doc: pynndb.Doc) -> int:
        filter_result = next(self.table.filter(index_name=index_name, lower=doc, page_size=1))
        return filter_result.count

    def create_filter_func(self, doc: pynndb.Doc) -> Callable[[pynndb.Doc], bool]:
        def filter_func(result_doc: pynndb.Doc):
            for attr in self.attrs_to_check:
                if result_doc[attr] != doc[attr]:
                    return False
            return True

        return filter_func

    def select_index_and_filter_func(self, doc: pynndb.Doc):
        if self.attrs_to_check is None:
            relevant_indexes = {}
            attr_names = set(doc.keys())
            for index_name in self.table.indexes(txn=self.txn):
                if self.database._index_attrs[(self.what.table_name, index_name)].issubset(attr_names):
                    relevant_indexes[index_name] = self.count(index_name=index_name, doc=doc)

            if relevant_indexes:
                self.index_name = min(relevant_indexes, key=relevant_indexes.get)
                self.attrs_to_check = attr_names - self.database._index_attrs[(self.what.table_name, self.index_name)]
            else:
                self.index_name = None
                self.attrs_to_check = attr_names

        self.filter_func = self.create_filter_func(doc) if self.attrs_to_check else None


class PynndbFilterStep(PynndbFilterStepBase):
    def __init__(
            self,
            database: Database,
            what: Type[Entity],
            attrs: Dict[str, Any],
            txn: lmdb.Transaction,
            filter_func: Optional[Callable[[pynndb.Doc], bool]] = None,
            attrs_to_check: Optional[Set[str]] = None,
            index_name: Optional[str] = None,
            is_root: bool = False,
    ):
        super().__init__(
            database=database,
            what=what,
            attrs=attrs,
            txn=txn,
            filter_func=filter_func,
            attrs_to_check=attrs_to_check,
            index_name=index_name,
            is_root=is_root,
        )
        self.docs = [self.doc] if is_root else []

    def input_attrs(self, **kwargs) -> None:
        self.docs.append(pynndb.Doc({**kwargs, **self.attrs}))

    def __iter__(self):
        for doc in self.docs:
            self.select_index_and_filter_func(doc)
            filter_result = self.table.filter(
                index_name=self.index_name,
                lower=doc,
                upper=doc,
                expression=self.filter_func,
                txn=self.txn,
            )
            yield from (self.what.from_doc(doc=result.doc, db=self.database, txn=self.txn) for result in filter_result)


class PynndbOutEStep(PynndbFilterStep):
    def __init__(
            self,
            database: Database,
            what: Type[Entity],
            attrs: Dict[str, Any],
            txn: lmdb.Transaction,
            filter_func: Optional[Callable[[pynndb.Doc], bool]] = None,
            attrs_to_check: Optional[Set[str]] = None,
            index_name: Optional[str] = None,
    ):
        super().__init__(
            database=database,
            what=what,
            attrs=attrs,
            txn=txn,
            filter_func=filter_func,
            attrs_to_check=attrs_to_check,
            index_name=index_name,
            is_root=False,
        )

    def input_node(self, node: Node) -> None:
        self.input_attrs(start_id=node.oid)

    def input(self, arg: Any) -> None:
        self.input_node(node=arg)
