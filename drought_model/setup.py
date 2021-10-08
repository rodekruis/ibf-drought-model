"""
Setup.py file.
Install once-off with:  "pip install ."
For development:        "pip install -e .[dev]"
"""
import setuptools


with open("requirements.txt") as f:
    install_requires = f.read().splitlines()

PROJECT_NAME = "drought_model"

setuptools.setup(
    name=PROJECT_NAME,
    version="0.2",
    author="Phuoc Phung",
    author_email="pphung@redcross.nl",
    description="App to post output IBF System end-point",
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    install_requires=install_requires,
    entry_points={
        'console_scripts': [
            f"run-drought-model = {PROJECT_NAME}.pipeline:main",
        ]
    }
)