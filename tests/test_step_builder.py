import string

from legdb.step import SourceStep, HasStep, PynndbFilterStep, PynndbEdgeOutStep, PynndbEdgeInStep, PynndbEdgeAllStep
from legdb.step_builder import StepBuilder
from conftest import Node, Edge


def test_step_repr():
    assert repr(SourceStep(Node)) == "node"
    assert repr(HasStep(language="en", label="test")) == "has(language='en', label='test')"


def test_step_builder_repr():
    step_builder = StepBuilder().source(Node).has(language="en", label="test").edge_all()
    assert repr(step_builder) == "node.has(language='en', label='test').edge_all()"


def test_step_builder_has(database, page_size):
    with database.read_transaction as txn:
        def new_step_builder():
            return StepBuilder(database=database, page_size=page_size, txn=txn)

        step_builder = new_step_builder().source(Node)
        assert [node.c for node in step_builder] == list(string.ascii_lowercase)
        assert step_builder._compiled_steps == [PynndbFilterStep(
            database=database,
            what=Node,
            attrs={},
            txn=txn,
        )]

        step_builder = new_step_builder().source(Node).has(c="a")
        assert [node.c for node in step_builder] == ["a"]
        assert step_builder._compiled_steps == [PynndbFilterStep(
            database=database,
            what=Node,
            attrs={"c": "a"},
            txn=txn,
        )]
        assert step_builder._compiled_steps[0].attrs_to_check == {frozenset({"c"}): frozenset()}
        assert step_builder._compiled_steps[0].index_names == {frozenset({"c"}): "by_c"}

        step_builder = new_step_builder().source(Node).has(ord_c_mod_2=0)
        assert [node.c for node in step_builder] == list(string.ascii_lowercase[1::2])
        assert step_builder._compiled_steps == [PynndbFilterStep(
            database=database,
            what=Node,
            attrs={"ord_c_mod_2": 0},
            txn=txn,
        )]
        assert step_builder._compiled_steps[0].attrs_to_check == {frozenset({"ord_c_mod_2"}): frozenset()}
        assert step_builder._compiled_steps[0].index_names == {frozenset({"ord_c_mod_2"}): "by_ord_c_mod_2"}

        step_builder = new_step_builder().source(Node).has(ord_c_mod_2=0).has(c="d")
        assert [node.c for node in step_builder] == ["d"]
        assert step_builder._compiled_steps == [PynndbFilterStep(
            database=database,
            what=Node,
            attrs={"c": "d", "ord_c_mod_2": 0},
            txn=txn,
        )]
        assert step_builder._compiled_steps[0].attrs_to_check == {frozenset({"c", "ord_c_mod_2"}): frozenset({"ord_c_mod_2"})}
        assert step_builder._compiled_steps[0].index_names == {frozenset({"c", "ord_c_mod_2"}): "by_c"}

        step_builder = new_step_builder().source(Node).has(ord_c_mod_2=0).has(ord_c_mod_3=0)
        assert [node.c for node in step_builder] == ['f', 'l', 'r', 'x']
        assert step_builder._compiled_steps == [PynndbFilterStep(
            database=database,
            what=Node,
            attrs={"ord_c_mod_2": 0, "ord_c_mod_3": 0},
            txn=txn,
        )]
        assert step_builder._compiled_steps[0].attrs_to_check == {frozenset({"ord_c_mod_3", "ord_c_mod_2"}): frozenset({"ord_c_mod_2"})}
        assert step_builder._compiled_steps[0].index_names == {frozenset({"ord_c_mod_3", "ord_c_mod_2"}): "by_ord_c_mod_3"}

        step_builder = new_step_builder().source(Node).has(ord_c_mod_4=0)
        assert [node.c for node in step_builder] == ["d", "h", "l", "p", "t", "x"]
        assert step_builder._compiled_steps == [PynndbFilterStep(
            database=database,
            what=Node,
            attrs={"ord_c_mod_4": 0},
            txn=txn,
        )]
        assert step_builder._compiled_steps[0].attrs_to_check == {frozenset({"ord_c_mod_4"}): frozenset({"ord_c_mod_4"})}
        assert step_builder._compiled_steps[0].index_names == {frozenset({"ord_c_mod_4"}): None}


def test_step_builder_edge_steps(database, page_size):
    with database.read_transaction as txn:
        def new_step_builder():
            return StepBuilder(database=database, edge_cls=Edge, page_size=page_size, txn=txn)

        step_builder = new_step_builder().source(Node).has(ord_c_mod_2=0, ord_c_mod_3=0).edge_in(w=-1.0)
        assert [edge.start.c for edge in step_builder] == ['g', 'm', 's', 'y']
        assert step_builder._compiled_steps == [
            PynndbFilterStep(
                database=database,
                what=Node,
                attrs={"ord_c_mod_2": 0, "ord_c_mod_3": 0},
                txn=txn,
            ),
            PynndbEdgeInStep(
                database=database,
                what=Edge,
                attrs={"w": -1.0},
                txn=txn,
            )
        ]
        assert step_builder._compiled_steps[0].attrs_to_check == {frozenset({"ord_c_mod_3", "ord_c_mod_2"}): frozenset({"ord_c_mod_2"})}
        assert step_builder._compiled_steps[0].index_names == {frozenset({"ord_c_mod_3", "ord_c_mod_2"}): "by_ord_c_mod_3"}
        assert step_builder._compiled_steps[1].attrs_to_check == {frozenset({"w", "end_id"}): frozenset({"end_id"})}
        assert step_builder._compiled_steps[1].index_names == {frozenset({"w", "end_id"}): "by_w"}

        step_builder = new_step_builder().source(Node).has(ord_c_mod_2=0, ord_c_mod_3=0).edge_out(w=1.0)
        assert [edge.end.c for edge in step_builder] == ['g', 'm', 's', 'y']
        assert step_builder._compiled_steps == [
            PynndbFilterStep(
                database=database,
                what=Node,
                attrs={"ord_c_mod_2": 0, "ord_c_mod_3": 0},
                txn=txn,
            ),
            PynndbEdgeOutStep(
                database=database,
                what=Edge,
                attrs={"w": 1.0},
                txn=txn,
            )
        ]
        assert step_builder._compiled_steps[0].attrs_to_check == {frozenset({"ord_c_mod_3", "ord_c_mod_2"}): frozenset({"ord_c_mod_2"})}
        assert step_builder._compiled_steps[0].index_names == {frozenset({"ord_c_mod_3", "ord_c_mod_2"}): "by_ord_c_mod_3"}
        assert step_builder._compiled_steps[1].attrs_to_check == {frozenset({"w", "start_id"}): frozenset({"start_id"})}
        assert step_builder._compiled_steps[1].index_names == {frozenset({"w", "start_id"}): "by_w"}

        step_builder = new_step_builder().source(Node).has(ord_c_mod_2=0, ord_c_mod_3=0).edge_all(w=1.0)
        edges = list(step_builder)
        assert len(edges) == 8
        for edge in edges:
            cs = ['f', 'l', 'r', 'x']
            assert edge.start.c in cs or edge.end.c in cs
            assert ord(edge.end.c) - ord(edge.start.c) == 1
        assert step_builder._compiled_steps == [
            PynndbFilterStep(
                database=database,
                what=Node,
                attrs={"ord_c_mod_2": 0, "ord_c_mod_3": 0},
                txn=txn,
            ),
            PynndbEdgeAllStep(
                database=database,
                what=Edge,
                attrs={"w": 1.0},
                txn=txn,
            )
        ]
        assert step_builder._compiled_steps[0].attrs_to_check == {frozenset({"ord_c_mod_3", "ord_c_mod_2"}): frozenset({"ord_c_mod_2"})}
        assert step_builder._compiled_steps[0].index_names == {frozenset({"ord_c_mod_3", "ord_c_mod_2"}): "by_ord_c_mod_3"}
        assert step_builder._compiled_steps[1].attrs_to_check == {
            frozenset({"w", "start_id"}): frozenset({"start_id"}),
            frozenset({"w", "end_id"}): frozenset({"end_id"}),
        }
        assert step_builder._compiled_steps[1].index_names == {
            frozenset({"w", "start_id"}): "by_w",
            frozenset({"w", "end_id"}): "by_w",
        }
