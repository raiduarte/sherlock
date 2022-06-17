from sherlock.classes_helper.pessoas import InserirDadosPessoas
import time
import datetime

import getopt
import sys


def importa_dados_de_arquivo():
    p_arquivo = ""
    p_n_processos = 4
    p_n_execute_many = 1000
    argv = sys.argv[1:]

    try:
        options, args = getopt.getopt(argv, "a:p:r:q:", ["arquivo =", "processos =", "paginacao =", "queries ="])
    except Exception:
        print("Parametro arquivo é obrigatório")
        raise Exception("Parametro arquivo é obrigatório")

    for name, value in options:
        if name in ['-a', '--arquivo']:
            p_arquivo = value
        elif name in ['-p', '--processos']:
            p_n_processos = value
        elif name in ['-q', '--queries']:
            p_n_execute_many = value

    print('Início do processo', datetime.datetime.now())
    iniciogeral = time.time()
    print(p_arquivo)
    isr_dados = InserirDadosPessoas()
    isr_dados.popula_tabela(p_arquivo, int(p_n_processos), int(p_n_execute_many))

    print('processo finalizado', datetime.datetime.now())

    fimgeral = time.time()
    print("tempo de processamento geral: " + str(fimgeral - iniciogeral))


if __name__ == '__main__':
    importa_dados_de_arquivo()
