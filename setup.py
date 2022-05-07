from setuptools import setup, find_packages

VERSION = "0.0.1"
DESCRIPTION = "A ray-based runtime that autoscales actors in a data parallel system"

with open("README.md", "r", encoding="utf-8") as fh:
    LONG_DESCRIPTION = fh.read()

setup(
    # the name must match the folder name 'verysimplemodule'
    name="dpa_autoscaler",
    version=VERSION,
    author="Atem Aguer",
    author_email="atemjohn@stanford.edu",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    package_dir={"": "src"},
    install_requires=[
        "ray==1.11.0",
        "mmh3",
        "pandas",
        "pytest",
    ],  # add any additional packages that
    license="https://www.mit.edu/~amini/LICENSE.md",
    keywords=["python", "ray", "data-parallel actors"],
    url="https://github.com/atemaguer/data-parallel-actor-autoscaler",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 3",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
    ],
    packages=find_packages(where="src"),
    python_requires=">=3.6",
)
