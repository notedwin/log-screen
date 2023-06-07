from setuptools import find_packages, setup

setup(
    name="data_cow",
    packages=find_packages(exclude=["data_cow_tests"]),
    install_requires=[
        "dagster",
        "dagster-postgres",
        "pandas",
        "sqlalchemy",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
