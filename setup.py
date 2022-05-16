from setuptools import setup, find_packages
import sherlock

setup(
    name='sherlock',
    version='0.1.2',
    packages=find_packages(),
    install_requires=[
        'mysqlclient',
    ],
    entry_points={
        'console_scripts': [
            'importar_dados = sherlock.main:importa_dados_de_arquivo',
        ],
    },
)