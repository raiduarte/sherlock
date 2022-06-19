import datetime
import time
import pymongo
from dbfread import DBF


class InserirDadosDB:
    PAGINACAO = 1000  # 100
    KEY_QRY_PARAMETRIZADA = "%s"  # "?" para SQL SERVER e "%s" para MySQL
    TABELA = 'veiculos'
    COLUNAS = None

    def __init__(self, tabela=None):
        if tabela is not None:
            self.TABELA = tabela

    def insere_dados(self, dados):
        myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        mydb = myclient["pessoas"]
        pessoas_colletion = mydb[self.TABELA]
        pessoas_colletion.insert_many(dados)

    def executa_funcao(self, func):
        return func()

    def tratamento_linha_to_list(self, linha) -> list:
        raise NotImplemented

    def popula_tabela(self, arquivo):
        inicio_temp = time.time()
        linhas_executemany = []
        contador = 1

        dbf_file = DBF(arquivo, encoding='iso-8859-1')

        for i, linha in enumerate(dbf_file):
            if i == 0:
                self.COLUNAS = dict(linha).keys()

            linha_valor = dict(linha)
            linhas_executemany.append(linha_valor)
            if contador == self.PAGINACAO:
                self.insere_dados(linhas_executemany)
                contador = 1
                linhas_executemany = []
            else:
                contador += 1

            if i % (5 * 10 ** 6) == 0:
                fim_temp = time.time()
                print('Fiz até a linha', i, datetime.datetime.now(), 'o último loop foi feito em',
                      str(fim_temp - inicio_temp))
                inicio_temp = time.time()

        self.insere_dados(linhas_executemany)

