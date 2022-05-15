from sherlock.importa_dados import popula_tabela
import time
import datetime

import getopt
import sys


def importa_dados_de_arquivo():
    p_arquivo = ""
    argv = sys.argv[1:]

    try:
        options, args = getopt.getopt(argv, "a:", ["arquivo ="])
    except Exception:
        print("Parametro arquivo é obrigatório")
        raise Exception("Parametro arquivo é obrigatório")

    for name, value in options:
        if name in ['-a', '--arquivo']:
            p_arquivo = value


    print('comecei agora', datetime.datetime.now())
    iniciogeral = time.time()
    print(p_arquivo)
    popula_tabela(p_arquivo)

    print('acabei agora', datetime.datetime.now())

    fimgeral = time.time()
    print("tempo de processamento geral: " + str(fimgeral - iniciogeral))


if __name__ == '__main__':
    importa_dados_de_arquivo()
