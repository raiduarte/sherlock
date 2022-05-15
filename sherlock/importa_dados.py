import datetime
import time
import multiprocessing
import functools
import MySQLdb

PAGINACAO = 1000  # 100
N_PROCESSOS = 24
N_EXECUTE_MANY = 1000  # 10000                                                                                                                               #no mysql %s no sqlserver
KEY_QRY_PARAMETRIZADA = "%s"  # "?" para SQL SERVER e "%s" para MySQL
Q_STR = f"insert into pessoas (cpf,nome,mae,dt_nasc,cidade,estado,bairro,logradouro,complemento,sexo,validado) values " + ", ".join([f"({', '.join([KEY_QRY_PARAMETRIZADA] * 11)})"] * PAGINACAO)

def insere_dados(dados, isql=Q_STR):
    con = MySQLdb.connect("localhost", "root", "sherlock1", "sherlock")

    cursor = con.cursor()
    cursor.executemany(isql, dados)
    cursor.close()
    con.close()


def executa_funcao(func):
    try:
        return func()
    except Exception as e:
        print(func)
        raise e


def popula_tabela(arquivo):
    inicio_temp = time.time()
    list_processes = []
    linhas_executemany = []
    linha_params = []
    contador = 1
    for i, linha in enumerate(open(arquivo, "r")):
        if linha != '\n':
            list_values = [linha[0:11].strip(), linha[11:52].strip(), linha[53:82].strip(), linha[83:91].strip(),
                          linha[166:184].strip(), linha[184:186].strip(), linha[143:158].strip(), linha[99:124].strip(),
                          linha[124:143].strip(), linha[186:187].strip(), '0']
            linha_params += list_values
            if contador == PAGINACAO:
                linhas_executemany.append(linha_params)

                if len(linhas_executemany) == N_EXECUTE_MANY:
                    list_processes.append(linhas_executemany)

                    if len(list_processes) == N_PROCESSOS:
                        partial_funs = []
                        for params_p in list_processes:
                            partial_funs.append(functools.partial(insere_dados, params_p))
                        with multiprocessing.Pool(N_PROCESSOS) as p:
                            p.map(executa_funcao, partial_funs)
                        list_processes = []

                    linhas_executemany = []

                contador = 1
                linha_params = []
            else:
                contador += 1

        if i % (5 * 10 ** 6) == 0:
            fim_temp = time.time()
            print('Fiz até a linha', i, datetime.datetime.now(), 'o último loop foi feito em', str(fim_temp - inicio_temp))
            inicio_temp = time.time()

    if linhas_executemany:
        list_processes.append(linhas_executemany)

    partial_funs = []
    for params_p in list_processes:
        partial_funs.append(functools.partial(insere_dados, params_p))
    with multiprocessing.Pool(len(list_processes)) as p:
        p.map(executa_funcao, partial_funs)

    params_restantes = int(len(linha_params) / 11)
    isql_restante = f"insert into pessoas (cpf,nome,mae,dt_nasc,cidade,estado,bairro,logradouro,complemento,sexo,validado) values " + ", ".join(
        [f"({', '.join([KEY_QRY_PARAMETRIZADA] * 11)})"] * params_restantes)
    insere_dados([linha_params], isql_restante)


if __name__ == '__main__':
    print('comecei agora', datetime.datetime.now())
    iniciogeral = time.time()

    popula_tabela()

    print('acabei agora', datetime.datetime.now())

    fimgeral = time.time()
    print("tempo de processamento geral: " + str(fimgeral - iniciogeral))
