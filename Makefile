init:
	uv sync

install:
	uv run python setup.py build_ext --inplace

clean:
	uv run python setup.py clean
