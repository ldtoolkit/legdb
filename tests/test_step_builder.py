import string

from legdb.step import SourceStep, HasStep, StepBuilder, PynndbFilterStep
from conftest import Node


def test_step_repr():
    assert repr(SourceStep(Node)) == "node"
    assert repr(HasStep(language="en", label="test")) == "has(language='en', label='test')"


def test_step_builder_repr():
    step_builder = StepBuilder().source(Node).has(language="en", label="test")
    assert repr(step_builder) == "node.has(language='en', label='test')"


def test_step_builder_compile(database):
    with database.read_transaction as txn:
        step_builder = StepBuilder(database=database, txn=txn).source(Node)
        step_builder._compile()
        assert step_builder._compiled_steps == [PynndbFilterStep(
            database=database,
            what=Node,
            attrs={},
            txn=txn,
        )]

        step_builder = StepBuilder(database=database, txn=txn).source(Node).has(language="en", label="test")
        step_builder._compile()
        assert step_builder._compiled_steps == [PynndbFilterStep(
            database=database,
            what=Node,
            attrs={"language": "en", "label": "test"},
            txn=txn,
        )]

        step_builder = StepBuilder(database=database, txn=txn)
        step_builder = step_builder.source(Node).has(language="en", label="test").has(sense=["n"])
        step_builder._compile()
        assert step_builder._compiled_steps == [PynndbFilterStep(
            database=database,
            what=Node,
            attrs={"language": "en", "label": "test", "sense": ["n"]},
            txn=txn,
        )]


def test_step_builder_iter(database):
    with database.read_transaction as txn:
        step_builder = StepBuilder(database=database, txn=txn).source(Node)
        assert [node.c for node in step_builder] == list(string.ascii_lowercase)

        step_builder = StepBuilder(database=database, txn=txn).source(Node).has(c="a")
        assert [node.c for node in step_builder] == ["a"]

        step_builder = StepBuilder(database=database, txn=txn).source(Node).has(ord_c_mod_2=0)
        assert [node.c for node in step_builder] == list(string.ascii_lowercase[1::2])
