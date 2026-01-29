from setuptools import setup, find_packages

setup(
    name='sqloader',
    version='0.1.3',
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
        "LogAssist>=1.1.1",
        "pymysql>=1.1.1"
    ],
)
