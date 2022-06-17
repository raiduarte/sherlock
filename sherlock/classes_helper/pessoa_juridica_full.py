from sherlock.importa_dados import InserirDadosDB


class InserirDadosPessoas(InserirDadosDB):
    TABELA = 'pessoas'
    COLUNAS = ["cpf", "nome", "mae", "dt_nasc", "cidade", "estado", "bairro", "logradouro", "complemento", "sexo",
               "validado"]

    def tratamento_linha_to_list(self, linha):
        return [linha[0:11].strip(), linha[11:52].strip(), linha[53:82].strip(), linha[83:91].strip(),
                              linha[166:184].strip(), linha[184:186].strip(), linha[143:158].strip(), linha[99:124].strip(),
                              linha[124:143].strip(), linha[186:187].strip(), '0']
