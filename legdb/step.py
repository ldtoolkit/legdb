from __future__ import annotations

from typing import Type, Any, Optional, Dict, Callable

import lmdb
import pynndb

from legdb import Entity, Database


class StepBuilder:
    def __init__(self, database: Optional[Database] = None, txn: Optional[lmdb.Transaction] = None) -> None:
        self._steps = []
        self._compiled_steps = []
        self._database = database
        self._txn = txn
        self._is_compiled = False

    def source(self, what: Type[Entity]) -> StepBuilder:
        if self._steps:
            raise ValueError("Step 'source' should be the first.")

        self._steps.append(SourceStep(what=what))
        return self

    def has(self, **kwargs) -> StepBuilder:
        self._steps.append(HasStep(**kwargs))
        return self

    def __repr__(self) -> str:
        return ".".join(repr(step) for step in self._steps)

    def _compile(self) -> None:
        if self._is_compiled:
            return
        self._compiled_steps = []
        step = self._steps.pop(0)
        if isinstance(step, SourceStep):
            step = PynndbFilterStep(database=self._database, what=step.what, attrs={}, txn=self._txn)
        else:
            raise ValueError("Step 'source' should be the first.")
        self._compiled_steps.append(step)
        while self._steps:
            next_step = self._steps.pop(0)
            if isinstance(step, PynndbFilterStep) and isinstance(next_step, HasStep):
                attrs = {**step.attrs, **next_step.attrs}
                step = PynndbFilterStep(database=self._database, what=step.what, attrs=attrs, txn=self._txn)
                self._compiled_steps.pop()
                self._compiled_steps.append(step)
            else:
                self._compiled_steps.append(step)
                step = next_step
        self._is_compiled = True

    def __iter__(self):
        self._compile()
        yield from self._compiled_steps[0]


class Step:
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


class PynndbStep(Step):
    def __init__(self, database: Database, txn: lmdb.Transaction) -> None:
        self.database = database
        self.txn = txn


class PynndbFilterStepBase(PynndbStep):
    def __init__(
            self,
            database: Database,
            what: Type[Entity],
            attrs: Dict[str, Any],
            txn: lmdb.Transaction,
            filter_func: Optional[Callable[[pynndb.Doc], bool]] = None,
            index_name: Optional[str] = None,
    ) -> None:
        super().__init__(database=database, txn=txn)
        self.attrs = attrs
        self.doc = pynndb.Doc(self.attrs)
        self.filter_func = filter_func
        self.index_name = index_name
        self.table: pynndb.Table = database._db.table(table_name=what.table_name, txn=txn)
        self.what = what

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, PynndbFilterStepBase):
            return NotImplemented
        return (self.attrs == other.attrs
                and self.index_name == other.index_name
                and self.database == other.database
                and self.txn == other.txn
                and self.what == other.what)

    def count(self, index_name: str) -> int:
        filter_result = next(self.table.filter(index_name=index_name, lower=self.doc, page_size=1))
        return filter_result.count

    def select_index_and_filter_func(self):
        def filter_func(doc: pynndb.Doc):
            for attr in attrs_to_check:
                if doc[attr] != self.doc[attr]:
                    return False
            return True

        relevant_indexes = {}
        attr_names = set(self.attrs.keys())
        for index_name in self.table.indexes(txn=self.txn):
            if self.database._index_attrs[(self.what, index_name)].issubset(attr_names):
                relevant_indexes[index_name] = self.count(index_name)

        if relevant_indexes:
            self.index_name = min(relevant_indexes, key=relevant_indexes.get)
            attrs_to_check = attr_names - self.database._index_attrs[(self.what, self.index_name)]
        else:
            self.index_name = None
            attrs_to_check = attr_names
        self.filter_func = filter_func if attrs_to_check else None


class PynndbFilterStep(PynndbFilterStepBase):
    def __iter__(self):
        self.select_index_and_filter_func()
        filter_result = self.table.filter(
            index_name=self.index_name,
            lower=self.doc,
            upper=self.doc,
            expression=self.filter_func,
            txn=self.txn,
        )
        yield from (self.what.from_doc(doc=result.doc, db=self.database, txn=self.txn) for result in filter_result)
