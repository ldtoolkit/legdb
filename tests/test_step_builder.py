import string

from legdb.step import SourceStep, HasStep, PynndbFilterStep, PynndbOutEStep, PynndbInEStep
from legdb.step_builder import StepBuilder
from conftest import Node, Edge


def test_step_repr():
    assert repr(SourceStep(Node)) == "node"
    assert repr(HasStep(language="en", label="test")) == "has(language='en', label='test')"


def test_step_builder_repr():
    step_builder = StepBuilder().source(Node).has(language="en", label="test")
    assert repr(step_builder) == "node.has(language='en', label='test')"


def test_step_builder_has(database):
    with database.read_transaction as txn:
        step_builder = StepBuilder(database=database, txn=txn).source(Node)
        assert [node.c for node in step_builder] == list(string.ascii_lowercase)
        assert step_builder._compiled_steps == [PynndbFilterStep(
            database=database,
            what=Node,
            attrs={},
            attrs_to_check=set(),
            txn=txn,
        )]

        step_builder = StepBuilder(database=database, txn=txn).source(Node).has(c="a")
        assert [node.c for node in step_builder] == ["a"]
        assert step_builder._compiled_steps == [PynndbFilterStep(
            database=database,
            what=Node,
            attrs={"c": "a"},
            attrs_to_check=set(),
            txn=txn,
            index_name="by_c",
        )]

        step_builder = StepBuilder(database=database, txn=txn).source(Node).has(ord_c_mod_2=0)
        assert [node.c for node in step_builder] == list(string.ascii_lowercase[1::2])
        assert step_builder._compiled_steps == [PynndbFilterStep(
            database=database,
            what=Node,
            attrs={"ord_c_mod_2": 0},
            attrs_to_check=set(),
            txn=txn,
            index_name="by_ord_c_mod_2",
        )]

        step_builder = StepBuilder(database=database, txn=txn).source(Node).has(ord_c_mod_2=0).has(c="d")
        assert [node.c for node in step_builder] == ["d"]
        assert step_builder._compiled_steps == [PynndbFilterStep(
            database=database,
            what=Node,
            attrs={"c": "d", "ord_c_mod_2": 0},
            attrs_to_check={"ord_c_mod_2"},
            txn=txn,
            index_name="by_c",
        )]

        step_builder = StepBuilder(database=database, txn=txn).source(Node).has(ord_c_mod_2=0).has(ord_c_mod_3=0)
        assert [node.c for node in step_builder] == ['f', 'l', 'r', 'x']
        assert step_builder._compiled_steps == [PynndbFilterStep(
            database=database,
            what=Node,
            attrs={"ord_c_mod_2": 0, "ord_c_mod_3": 0},
            attrs_to_check={"ord_c_mod_2"},
            txn=txn,
            index_name="by_ord_c_mod_3",
        )]

        step_builder = StepBuilder(database=database, txn=txn).source(Node).has(ord_c_mod_4=0)
        assert [node.c for node in step_builder] == ["d", "h", "l", "p", "t", "x"]
        assert step_builder._compiled_steps == [PynndbFilterStep(
            database=database,
            what=Node,
            attrs={"ord_c_mod_4": 0},
            attrs_to_check={"ord_c_mod_4"},
            txn=txn,
        )]


def test_step_builder_edge_steps(database):
    with database.read_transaction as txn:
        step_builder = StepBuilder(database=database, edge_cls=Edge, txn=txn)
        step_builder = step_builder.source(Node).has(ord_c_mod_2=0, ord_c_mod_3=0).in_e(w=-1.0)
        assert [edge.start.c for edge in step_builder] == ['g', 'm', 's', 'y']
        assert step_builder._compiled_steps == [
            PynndbFilterStep(
                database=database,
                what=Node,
                attrs={"ord_c_mod_2": 0, "ord_c_mod_3": 0},
                attrs_to_check={"ord_c_mod_2"},
                txn=txn,
                index_name="by_ord_c_mod_3",
            ),
            PynndbInEStep(
                database=database,
                what=Edge,
                attrs={"w": -1.0},
                attrs_to_check={"end_id"},
                txn=txn,
                index_name="by_w",
            )
        ]

        step_builder = StepBuilder(database=database, edge_cls=Edge, txn=txn)
        step_builder = step_builder.source(Node).has(ord_c_mod_2=0, ord_c_mod_3=0).out_e(w=1.0)
        assert [edge.end.c for edge in step_builder] == ['g', 'm', 's', 'y']
        assert step_builder._compiled_steps == [
            PynndbFilterStep(
                database=database,
                what=Node,
                attrs={"ord_c_mod_2": 0, "ord_c_mod_3": 0},
                attrs_to_check={"ord_c_mod_2"},
                txn=txn,
                index_name="by_ord_c_mod_3",
            ),
            PynndbOutEStep(
                database=database,
                what=Edge,
                attrs={"w": 1.0},
                attrs_to_check={"start_id"},
                txn=txn,
                index_name="by_w",
            )
        ]

