from setuptools import setup, find_packages

with open("requirements.txt") as f:
    requirements = f.readlines()

long_description = "network builder is a ."

setup(
    name="combine_gtfs_feeds",
    version="0.1.8",
    author="psrc staff",
    author_email="scoe@psrc.org",
    url="https://github.com/psrc/network_builder",
    description="Build Soundcast Networks.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="MIT",
    packages=find_packages(),
    python_requires=">3.11",
    entry_points={
        "console_scripts": ["network_builder = network_builder.cli.main:main"]
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    keywords="GTFS",
    install_requires=requirements,
    zip_safe=False,
)
