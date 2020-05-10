from __future__ import annotations

from dataclasses import dataclass, InitVar, field
from typing import Optional, Callable, Dict, Mapping, TypeVar, Type, TYPE_CHECKING

from pynndb import Doc
from mashumaro.serializer.base import DataClassDictMixin
from types import MappingProxyType

from legdb.database import Database

if TYPE_CHECKING:
    from legdb.pynndb_types import Transaction


DEFAULT_DICT_PARAMS = {
    "use_bytes": True,
    "use_enum": False,
    "use_datetime": False
}
Encoder = Callable[[Dict], Doc]
Decoder = Callable[[Doc], Dict]
T = TypeVar("T", bound="Entity")


@dataclass
class Entity(DataClassDictMixin):
    oid: Optional[bytes] = field(default=None, repr=False)
    db: InitVar[Optional[Database]] = None
    _db = None
    _skip_on_to_doc = []
    table_name = None

    def __post_init__(self, db: Optional[Database]):
        self.connect(db)
        if self._db is not None:
            self.load()

    def connect(self, db: Optional[Database] = None) -> None:
        self._db = db

    def to_doc(self, dict_params: Optional[Mapping] = MappingProxyType({})) -> Doc:
        d = self.to_dict(**dict(DEFAULT_DICT_PARAMS, **dict_params))
        oid = d.pop("oid", None)
        for key in self._skip_on_to_doc:
            d.pop(key, None)
        # LMDB doesn't allow to have empty keys, replace them with dash
        for key, value in d.items():
            if value == "":
                d[key] = "-"
        result = Doc(d)
        result.oid = oid
        return result

    @classmethod
    def from_doc(
            cls: Type[T],
            doc: Optional[Doc],
            dict_params: Optional[Mapping] = MappingProxyType({}),
    ) -> Optional[T]:
        if doc is None:
            return None
        d = dict(doc)
        # LMDB doesn't allow to have empty keys, replace dashes back to empty string
        for key, value in d.items():
            if value == "-":
                d[key] = ""
        result = cls.from_dict(d, **dict(DEFAULT_DICT_PARAMS, **dict_params))
        result.oid = doc.oid
        return result

    @classmethod
    def from_db_and_doc(
            cls: Type[T],
            db: Database,
            doc: Optional[Doc],
            dict_params: Optional[Mapping] = MappingProxyType({}),
    ) -> Optional[T]:
        if doc is None:
            return None
        result = cls.from_doc(doc=doc, dict_params=dict_params)
        result.connect(db)
        return result

    def _raise_when_not_connected_to_database(self) -> None:
        if self._db is None:
            raise ValueError(
                f"{type(self)} not connected to the database; "
                f"please use {type(self)}.connect method or retrieve it directly from database."
            )

    def load(self, txn: Optional[Transaction] = None) -> None:
        self._raise_when_not_connected_to_database()

    def save(self, txn: Optional[Transaction] = None) -> None:
        self._raise_when_not_connected_to_database()
        self._db.save(self, txn=txn)


@dataclass
class Node(Entity):
    table_name = "node"


@dataclass
class Edge(Entity):
    start: Optional[Node] = None
    end: Optional[Node] = None
    start_id: Optional[bytes] = field(default=None, repr=False)
    end_id: Optional[bytes] = field(default=None, repr=False)
    table_name = "edge"
    _skip_on_to_doc = ["start", "end"]
    _node_class = None

    def __post_init__(self, db: Optional[Database] = None):
        if self.start is not None:
            self.start_id = self.start.oid
        if self.end is not None:
            self.end_id = self.end.oid
        super().__post_init__(db=db)

    def load(self, txn: Optional[Transaction] = None) -> None:
        super().load(txn=txn)
        if self.start is None or self.start == Node():
            self.start = self._node_class.from_doc(self._db.node_table.get(oid=self.start_id, txn=txn))
        if self.end is None or self.end == Node():
            self.end = self._node_class.from_doc(self._db.node_table.get(oid=self.end_id, txn=txn))
