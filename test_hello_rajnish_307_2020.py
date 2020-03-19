from hello_rajnish_307_2020 import say_hello


def test_without_param():
    assert say_hello() == "Hello World"


def test_with_param():
    assert say_hello("Raj") == "Hello Raj"
