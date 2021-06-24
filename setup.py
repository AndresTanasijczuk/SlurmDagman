""" Setuptools-based setup module for SlurmDagman """

from setuptools import setup, find_packages
import io
import os

def read(rel_path):
    here = os.path.abspath(os.path.dirname(__file__))
    with io.open(os.path.join(here, rel_path), 'r', encoding="utf-8") as fp:
        return fp.read()

def get_version(rel_path):
    for line in read(rel_path).splitlines():
        if line.startswith('__version__'):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    else:
        raise RuntimeError("Unable to find version string.")

pkgName = "SlurmDagman"

long_description = read("README.md")

setup(
    name=pkgName,
    version=get_version("lib/SlurmDagman/__init__.py"),

    description="Application to run a DAG with SLURM.",
    long_description=long_description,
    long_description_content_type='text/markdown',

    url="https://github.com/AndresTanasijczuk/SlurmDagman",

    author="Andres Tanasijczuk (Université catholique de Louvain)",
    author_email="cp3-support@listes.uclouvain.be",
    license="GPL-3.0-or-later",

    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Scientific/Engineering :: Physics',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Distributed Computing',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],

    keywords=['slurm','dag'],

    package_dir={'' : 'lib'},
    packages=find_packages('lib'),
    scripts=[os.path.join(root, item) for root, subFolder, files in os.walk('bin') for item in files],
    # This works together with MANIFEST.in for sdist.
    #include_package_data=True,
    # This is for bdist_wheel.
    #data_files=[('./libexec',['scripts/slurm_dagman'])],
    data_files=[(os.path.join('etc', pkgName), [os.path.join('etc', 'SlurmDagman.conf')])],
)
