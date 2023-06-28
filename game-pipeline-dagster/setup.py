from setuptools import find_packages, setup

setup(
    name="game_pipeline_dagster",
    packages=find_packages(exclude=["game_pipeline_dagster_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
