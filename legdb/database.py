from __future__ import annotations

import dataclasses
from enum import Enum
from itertools import product
from pathlib import Path
from subprocess import call
from types import MappingProxyType
from typing import Union, Optional, Mapping, Any, Generator, List, TYPE_CHECKING, Type, TypeVar, Callable

import pynndb

from legdb import entity
from legdb.pynndb_types import CompressionType, Transaction
from legdb.index import IndexBy
import legdb

if TYPE_CHECKING:
    from legdb.entity import Entity, Edge


T = TypeVar("T", bound="Entity")


class DbOpenMode(Enum):
    CREATE = 'create'
    READ_WRITE = 'read'


class Database:
    def __init__(
            self,
            path: Union[Path, str],
            db_open_mode: DbOpenMode = DbOpenMode.READ_WRITE,
            config: Optional[Mapping[str, Any]] = None
    ):
        self._path = Path(path)
        self._db_open_mode = db_open_mode
        self._db = pynndb.Database()
        if config is None:
            config = {}
        # config["readonly"] = self._db_open_mode == DbOpenMode.READ
        self._config = config
        self._db.configure(self._config)
        self._db.open(str(self._path))
        with self._db.write_transaction as txn:
            self.node_table = self._db.table(entity.Node.table_name, txn=txn)
            self.node_table.APPEND_MODE = False
            self.edge_table = self._db.table(entity.Edge.table_name, txn=txn)
            self.edge_table.APPEND_MODE = False
            self.edge_table.ensure(IndexBy.start_id_end_id.value, "!{start_id}|{end_id}", duplicates=True, txn=txn)
            self.edge_table.ensure(IndexBy.start_id.value, "{start_id}", duplicates=True, txn=txn)
            self.edge_table.ensure(IndexBy.end_id.value, "{end_id}", duplicates=True, txn=txn)

    def ensure_index(
            self,
            what: Type[Entity],
            name: str,
            func: Optional[str] = None,
            duplicates: bool = False,
            force: bool = False,
            txn: Optional[Transaction] = None,
    ) -> pynndb.Index:
        return self._db[what.table_name].ensure(index_name=name, func=func, duplicates=duplicates, force=force, txn=txn)

    def sync(self, force: bool = True):
        self._db.sync(force=force)

    @property
    def read_transaction(self) -> Transaction:
        return self._db.read_transaction

    @property
    def write_transaction(self) -> Transaction:
        return self._db.write_transaction

    def save(self, entity: T, txn: Optional[Transaction] = None, return_oid: bool = False) -> Optional[Union[T, bytes]]:
        table = self._db[entity.table_name]
        doc = entity.to_doc()
        if entity.oid is None:
            saved_doc = table.append(doc, txn=txn)
            if return_oid:
                return saved_doc.oid
            else:
                return type(entity).from_db_and_doc(db=self, doc=saved_doc)
        else:
            table.save(doc, txn=txn)

    def get(
            self,
            cls: Type[T],
            oid: Optional[bytes],
            txn: Optional[Transaction] = None
    ) -> Optional[T]:
        if oid is None:
            return None
        return cls.from_db_and_doc(db=self, doc=self._db[cls.table_name].get(oid=oid, txn=txn))

    def range(
            self,
            lower: Optional[T]=None,
            upper: Optional[T]=None,
            index_name: Optional[str]=None,
            oids_only: bool=False,
            inclusive: bool=True,
            txn: Optional[Transaction]=None) -> Generator[T, None, None]:
        types = {type(x) for x in [lower, upper] if x is not None}
        if len(types) > 1:
            raise TypeError("lower and upper should be None or of same type")
        cls,  = types
        if index_name is None:
            indexes_names = self.get_indexes(entity=lower)
        else:
            indexes_names = [index_name]
        table = self._db[cls.table_name]
        lower_doc = lower.to_doc() if lower is not None else None
        upper_doc = upper.to_doc() if upper is not None else None

        if len(indexes_names) == 1:
            for doc in table.range(
                index_name=indexes_names[0],
                lower=lower_doc,
                upper=upper_doc,
                inclusive=inclusive,
                txn=txn,
            ):
                yield cls.from_db_and_doc(db=self, doc=doc)
        else:
            result_oids = None
            for index_name in indexes_names:
                oids = set()
                for cursor in table.range(
                    index_name=index_name,
                    lower=lower_doc,
                    upper=upper_doc,
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
                    yield cls.from_db_and_doc(db=self, doc=table.get(oid, txn=txn))

    def _expand_edge(self, edge: Edge, txn: Optional[Transaction] = None) -> List[Edge]:
        def expand_start_and_end(edge):
            if edge.start is not None and not edge.start.is_bound:
                start_node_ids = [oid.encode() for oid in self.seek(edge.start, oids_only=True, txn=txn)]
            else:
                start_node_ids = [edge.start_id]
            if edge.end is not None and not edge.end.is_bound:
                end_node_ids = [oid.encode() for oid in self.seek(edge.end, oids_only=True, txn=txn)]
            else:
                end_node_ids = [edge.end_id]
            return [
                dataclasses.replace(edge, start=None, end=None, start_id=start_id, end_id=end_id, db=None)
                for start_id, end_id in product(start_node_ids, end_node_ids)
            ]

        # If we had something like Edge(Node(...))
        if isinstance(edge.oid, legdb.entity.Node):
            start_or_end = edge.oid
            edge.oid = None
            result = []
            result.extend(expand_start_and_end(dataclasses.replace(edge, start=start_or_end, db=None)))
            result.extend(expand_start_and_end(dataclasses.replace(edge, end=start_or_end, db=None)))
            return result
        elif (edge.start is not None and not edge.start.is_bound
              or edge.end is not None and not edge.end.is_bound):
            return expand_start_and_end(edge)
        else:
            return [edge]

    def seek(
            self,
            entity: Entity,
            index_name: Optional[str] = None,
            oids_only: bool = False,
            txn: Optional[Transaction] = None
    ):
        cls = type(entity)
        table = self._db[entity.table_name]

        if isinstance(entity, legdb.entity.Edge):
            entities = self._expand_edge(entity, txn=txn)
        else:
            entities = [entity]

        if len(entities) == 1:
            if index_name is None:
                indexes_names = self.get_indexes(entity=entity)
            else:
                indexes_names = [index_name]
            if len(indexes_names) == 1:
                doc = entity.to_doc()
                index_name, = indexes_names
                if oids_only:
                    oids = set()
                    for cursor in table.seek(index_name=index_name, doc=doc, keyonly=True, txn=txn):
                        oids.add(cursor.val)
                    yield from oids
                else:
                    for doc in table.seek(index_name=index_name, doc=doc, txn=txn):
                        yield cls.from_db_and_doc(db=self, doc=doc)
                return

        result_oids = set()

        for entity in entities:
            oids_for_entity = None
            doc = entity.to_doc()
            if index_name is None:
                indexes_names = self.get_indexes(entity=entity)
            else:
                indexes_names = [index_name]
            for i in indexes_names:
                oids = set()
                for cursor in table.seek(index_name=i, doc=doc, keyonly=True, txn=txn):
                    oids.add(cursor.val)
                if oids_for_entity is None:
                    oids_for_entity = oids
                else:
                    oids_for_entity.intersection_update(oids)
            result_oids.update(oids_for_entity)

        if result_oids is None:
            result_oids = []
        if oids_only:
            yield from result_oids
        else:
            for oid in result_oids:
                yield cls.from_db_and_doc(db=self, doc=table.get(oid, txn=txn))

    def seek_one(self, entity: Entity, index_name: Optional[str] = None, txn: Optional[Transaction] = None):
        if index_name is None:
            index_name = self.get_indexes(entity=entity)[0]
        cls = type(entity)
        return cls.from_db_and_doc(
            db=self,
            doc=self._db[entity.table_name].seek_one(index_name=index_name, doc=entity.to_doc(), txn=txn),
        )

    def find(
            self,
            what: Type[T],
            index_name: Optional[str] = None,
            expression: Optional[Callable[[T], bool]] = None,
            txn: Optional[Transaction] = None,
     ) -> Generator[T, None, None]:
        table = self._db[what.table_name]
        for doc in table.find(index_name=index_name, txn=txn):
            entity = what.from_db_and_doc(db=self, doc=doc)
            if callable(expression) and not expression(entity):
                continue
            yield entity

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

    def vacuum(self) -> None:
        self._db.close()
        dump_file_path = self._path.with_suffix(".tmp.db.dump")
        compressed_file = open(dump_file_path, "w")
        call(["mdb_dump", "-n", "-a", self._path], stdout=compressed_file)
        self._path.unlink()
        call(["mdb_load", "-n", "-f", dump_file_path, self._path])
        dump_file_path.unlink()
        config = self._config.copy()
        # config["map_size"] = self._path.stat().st_size
        config["map_size"] = 2**34  # 16 GiB FIXME
        self.__init__(path=self._path, db_open_mode=self._db_open_mode, config=config)

    def get_indexes(self, entity: Entity) -> List[str]:
        raise NotImplementedError("LegDB.get_index_name should be overridden in subclasses")