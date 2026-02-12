from setuptools import setup, find_packages

setup(
    name='sqloader',
    version='0.2.4',
    description='py_sqloader package',
    author='horrible-gh',
    author_email='shinjpn1@gmail.com',
    url='https://github.com/horrible-gh/py_sqloader.git',
    packages=find_packages(),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
    python_requires='>=3.6',
    install_requires=[
        "pymysql>=1.1.1",  # MySQL sync is always included
    ],
    extras_require={
        "postgresql": ["psycopg2-binary>=2.9.0"],
        "async-mysql": ["aiomysql>=0.2.0"],
        "async-postgresql": ["asyncpg>=0.27.0"],
        "async-sqlite": ["aiosqlite>=0.19.0"],
        "all": [
            "psycopg2-binary>=2.9.0",
            "aiomysql>=0.2.0",
            "asyncpg>=0.27.0",
            "aiosqlite>=0.19.0",
        ],
    },
)
