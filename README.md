#Hello World
This is an example project demonstrating how to publish a python module to PyPI.

##Installation
Run the following to install:
```python pip install hello_world```

##Usage
```python from hello_world import say_hello

#Generate "Hello World"
say_hello()
#Generate "Hello Raj"
say_hello("Raj)
```

#Developing Hello World
To install helloworld, along with the tools you need to develop and run tests, run the following in your virtualenv:
```bash
$ pip install -e .[dev]
```
