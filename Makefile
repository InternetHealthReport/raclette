init:
	pip install -r requirements.txt

install:
	python setup.py build_ext --inplace

clean:
	python setup.py clean
