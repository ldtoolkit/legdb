from __future__ import annotations

from typing import Type, Any, Optional, Dict

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


class HasStep(Step):
    def __init__(self, **kwargs) -> None:
        super().__init__()
        self.attrs = kwargs

    def __repr__(self) -> str:
        attrs_str = ", ".join(f"{key}={value!r}" for key, value in self.attrs.items())
        return f"has({attrs_str})"


class PynndbStep(Step):
    def __init__(self, database: Database, txn: lmdb.Transaction) -> None:
        self.database = database
        self.txn = txn


class PynndbFilterStep(PynndbStep):
    def __init__(
            self,
            database: Database,
            what: Type[Entity],
            attrs: Dict[str, Any],
            txn: lmdb.Transaction,
    ) -> None:
        super().__init__(database=database, txn=txn)
        self.table: pynndb.Table = database._db.table(table_name=what.table_name, txn=txn)
        self.attrs = attrs
        self.what = what

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, PynndbFilterStep):
            return NotImplemented
        return (self.attrs == other.attrs
                and self.database == other.database
                and self.txn == other.txn
                and self.what == other.what)

    def __iter__(self):
        entity = self.what(**self.attrs)
        doc = entity.to_doc()
        index_names = self.database.get_indexes(entity)
        index_name = index_names[0] if index_names else None
        yield from (self.what.from_doc(doc=result.doc, db=self.database, txn=self.txn)
                    for result in self.table.filter(index_name=index_name, lower=doc, upper=doc, txn=self.txn))
