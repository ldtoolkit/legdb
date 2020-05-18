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

    @property
    def is_bound(self):
        return self._db is not None

    def connect(self, db: Optional[Database] = None) -> None:
        self._db = db
        if self.is_bound:
            self.load()

    def to_doc(self, dict_params: Optional[Mapping] = MappingProxyType({})) -> Doc:
        d = self.to_dict(**dict(DEFAULT_DICT_PARAMS, **dict_params))
        oid = d.pop("oid", None)
        for key in self._skip_on_to_doc:
            d.pop(key, None)
        result = Doc(d)
        result.oid = oid
        return result

    @classmethod
    def from_doc(cls: Type[T], doc: Optional[Doc], db: Optional[Database] = None) -> Optional[T]:
        if doc is None:
            return None
        result = cls.from_dict(dict(doc), **DEFAULT_DICT_PARAMS)
        result.oid = doc.oid
        result.connect(db)
        return result

    def _raise_when_unbound(self) -> None:
        if not self.is_bound:
            raise ValueError(
                f"{type(self)} not connected to the database; "
                f"please use {type(self)}.connect method or retrieve it directly from database."
            )

    def load(self, txn: Optional[Transaction] = None) -> None:
        self._raise_when_unbound()

    def save(self, txn: Optional[Transaction] = None) -> None:
        self._raise_when_unbound()
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
        if self.start is not None and self.start_id is None:
            self.start_id = self.start.oid
        if self.end is not None and self.end_id is None:
            self.end_id = self.end.oid
        super().__post_init__(db=db)

    def load(self, txn: Optional[Transaction] = None) -> None:
        super().load(txn=txn)
        if (self.start is None or self.start == Node()) and self.start_id is not None:
            self.start = self._node_class.from_doc(self._db.node_table.get(oid=self.start_id, txn=txn))
        if (self.end is None or self.end == Node()) and self.end_id is not None:
            self.end = self._node_class.from_doc(self._db.node_table.get(oid=self.end_id, txn=txn))
