from __future__ import annotations

import functools
from enum import Enum
from pathlib import Path
from typing import Union, Optional, Mapping, Any, Generator, List, TYPE_CHECKING, Type, TypeVar, Callable, Collection

import pynndb
from pynndb import write_transaction

from legdb import entity
from legdb.pynndb_types import CompressionType, Transaction
from legdb.index import IndexBy

if TYPE_CHECKING:
    from legdb.entity import Entity


T = TypeVar("T", bound="Entity")


class DbOpenMode(Enum):
    CREATE = 'create'
    READ_WRITE = 'read'


def wrap_reader_yield(func: Callable) -> Callable:
    @functools.wraps(func)
    def wrapped(*args, **kwargs) -> Generator[Any, None, None]:
        if 'txn' in kwargs and kwargs['txn']:
            yield from func(*args, **kwargs)
        else:
            with args[0].read_transaction as kwargs['txn']:
                yield from func(*args, **kwargs)
    return wrapped


class Database:
    def __init__(
            self,
            path: Union[Path, str],
            db_open_mode: DbOpenMode = DbOpenMode.READ_WRITE,
            config: Optional[Mapping[str, Any]] = None,
    ):
        @write_transaction
        def open_tables(db, txn=None):
            self.node_table = self._db.table(entity.Node.table_name, txn=txn)
            self.edge_table = self._db.table(entity.Edge.table_name, txn=txn)
            self.ensure_index(
                entity.Edge,
                IndexBy.start_id_end_id.value,
                ["start_id", "end_id"],
                "!{start_id}|{end_id}",
                duplicates=True,
                txn=txn,
            )
            self.ensure_index(entity.Edge, IndexBy.start_id.value, ["start_id"], "{start_id}", duplicates=True, txn=txn)
            self.ensure_index(entity.Edge, IndexBy.end_id.value, ["end_id"], "{end_id}", duplicates=True, txn=txn)

        self._path = Path(path)
        self._db_open_mode = db_open_mode
        self._db = pynndb.Database()
        if config is None:
            config = {}
        # config["readonly"] = self._db_open_mode == DbOpenMode.READ
        self._config = config
        self._db.configure(self._config)
        self._db.open(str(self._path))
        self._index_attrs = {}
        self._index_names_by_attr_names = {}
        self._attrs_to_check_by_attr_names = {}
        open_tables(self._db)

    def __getstate__(self):
        state = self.__dict__.copy()
        del state["_db"]
        del state["node_table"]
        del state["edge_table"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.__init__(path=self._path, db_open_mode=self._db_open_mode, config=self._config)

    def ensure_index(
            self,
            what: Type[Entity],
            name: str,
            attrs: Collection[str],
            func: str,
            duplicates: bool = False,
            force: bool = False,
            txn: Optional[Transaction] = None,
    ) -> pynndb.Index:
        self._index_attrs[(what.table_name, name)] = set(attrs)
        return self._db[what.table_name].ensure(index_name=name, func=func, duplicates=duplicates, force=force, txn=txn)

    def sync(self, force: bool = True):
        self._db.sync(force=force)

    @property
    def read_transaction(self) -> Transaction:
        return self._db.read_transaction

    @property
    def write_transaction(self) -> Transaction:
        return self._db.write_transaction

    def save(self, entity: T, txn: Optional[Transaction] = None, return_oid: bool = False) -> Optional[Union[T, str]]:
        table = self._db[entity.table_name]
        doc = entity.to_doc()
        if entity.oid is None:
            saved_doc = table.append(doc, txn=txn)
            if return_oid:
                return saved_doc.key
            else:
                return type(entity).from_doc(db=self, doc=saved_doc, txn=txn)
        else:
            table.save(doc, txn=txn)

    def compress(
            self,
            what: Type[T],
            training_samples: List[bytes],
            compression_type: CompressionType = CompressionType.ZSTD,
            compression_level: int = 3,
            training_dict_size: int = 4096,
            threads: int = -1,
            txn: Optional[Transaction] = None,
    ) -> None:
        if compression_type == CompressionType.ZSTD:
            self._db[what.table_name].zstd_train(
                training_samples=training_samples,
                training_dict_size=training_dict_size,
                threads=threads,
                txn=txn,
            )
        self._db[what.table_name].close()
        self._db[what.table_name].open(
            compression_type=compression_type,
            compression_level=compression_level,
            txn=txn,
        )

    def get(
            self,
            cls: Type[T],
            oid: Optional[bytes],
            txn: Optional[Transaction] = None
    ) -> Optional[T]:
        if oid is None:
            return None
        return cls.from_doc(db=self, doc=self._db[cls.table_name].get(oid=oid, txn=txn))
