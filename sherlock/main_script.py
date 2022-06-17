import datetime
import time

from sherlock.importa_dados import InserirDadosDB


def importa_dados_de_arquivo(p_arquivo, nome_tabela, p_n_processos=4, p_n_execute_many=1000):
    print('Início do processo', datetime.datetime.now())
    iniciogeral = time.time()
    print(p_arquivo)
    isr_dados = InserirDadosDB(nome_tabela)
    isr_dados.popula_tabela(p_arquivo)

    print('processo finalizado', datetime.datetime.now())

    fimgeral = time.time()
    print("tempo de processamento geral: " + str(fimgeral - iniciogeral))

# if __name__ == '__main__':
# importa_dados_de_arquivo(p_arquivo)
