# Libs
import os
import pandas as pd
from download_transformacao_CNPJ import EXTRATOR_CNPJ
from pyspark.sql.functions import concat_ws, lpad, coalesce, when, lit
from time import localtime, strftime
current_dir = os.path.dirname(os.path.abspath(__file__))
dir_dados = os.path.join(current_dir, r"dados")

# Se passar baixar_e_extrair como false, precisa do nome do arquivo.
print(f'Começando a buscar os dados: {strftime("%d/%m/%Y %H:%M:%S", localtime())}')
ESTABELECIMENTOS, spark = EXTRATOR_CNPJ(baixar_e_extrair=False, nome_arquivo="Estabelecimentos").run()
print(f'Termino da coleta dos ESTABELECIMENTOS: {strftime("%d/%m/%Y %H:%M:%S", localtime())}')
EMPRESAS, spark = EXTRATOR_CNPJ(baixar_e_extrair=False, nome_arquivo="Empresas").run()
print(f'Termino da coleta dos EMPRESAS: {strftime("%d/%m/%Y %H:%M:%S", localtime())}')
MUNICIPIOS, spark = EXTRATOR_CNPJ(baixar_e_extrair=False, nome_arquivo="Municipios").run()
print(f'Final da coleta dos dados: {strftime("%d/%m/%Y %H:%M:%S", localtime())}')

# Dicionário com os cnaes que vamos buscar.
CNAES = {
        5612100:'Serviços ambulantes de alimentação',
        5611201:'Restaurantes e similares',
        5611203:'Lanchonetes casas de chá de sucos e similares',
        5611204:'Bares e outros estabelecimentos especializados em servir bebidas sem entretenimento',
        5611205:'Bares e outros estabelecimentos especializados em servir bebidas com entretenimento',
        4721102: 'Padaria e confeitaria com predominância de revenda'
        }

# Define função de filtragem dos dados no sparkSession
def filtragem_cnae_df(cod_cnae:int):
    # cria um dataframe com base nos filtros aplicados
    from pyspark.sql.functions import concat_ws, lpad, coalesce, when
    dataframe = (
        ESTABELECIMENTOS
        .join(EMPRESAS, "CNPJ_BASE", "right")
        .join(MUNICIPIOS, "CODIGO_MUNICIPIO", "right")
        .where(
            (ESTABELECIMENTOS["CNAE_PRINCIPAL"] == f"{cod_cnae}") &
            (ESTABELECIMENTOS["SITUACAO_CADASTRAL"].isin([2, 3, 4]))
        )
        .select(
            concat_ws("", 
                lpad(ESTABELECIMENTOS["CNPJ_BASE"].cast("bigint"), 8, "0"), 
                lpad(ESTABELECIMENTOS["CNPJ_ORDEM"], 4, "0"), 
                lpad(ESTABELECIMENTOS["CNPJ_DV"], 2, "0")
            ).alias("CNPJ"),
            when(EMPRESAS.RAZAO_SOCIAL.isNull(), None).otherwise(EMPRESAS.RAZAO_SOCIAL).alias("RAZAO_SOCIAL"),
            when(ESTABELECIMENTOS.NOME_FANTASIA.isNull(), None).otherwise(ESTABELECIMENTOS.NOME_FANTASIA).alias("NOME_FANTASIA"),
            ESTABELECIMENTOS.SITUACAO_CADASTRAL,
            ESTABELECIMENTOS.DATA_SITUACAO_CADASTRAL,
            ESTABELECIMENTOS.DATA_INICIO_ATIVIDADE,
            ESTABELECIMENTOS.CNAE_PRINCIPAL,
            concat_ws(" ",
                when(ESTABELECIMENTOS.TIPO_LOGRADOURO.isNull(), None).otherwise(ESTABELECIMENTOS.TIPO_LOGRADOURO),
                when(ESTABELECIMENTOS.LOGRADOURO.isNull(), None).otherwise(ESTABELECIMENTOS.LOGRADOURO),
                when(ESTABELECIMENTOS.NUMERO.isNull(), None).otherwise(ESTABELECIMENTOS.NUMERO),
                when(ESTABELECIMENTOS.COMPLEMENTO.isNull(), None).otherwise(ESTABELECIMENTOS.COMPLEMENTO)
            ).alias("ENDERECO"),
            ESTABELECIMENTOS.BAIRRO,
            MUNICIPIOS.NOME_MUNICIPIO.alias("CIDADE"),
            ESTABELECIMENTOS.UF,
            ESTABELECIMENTOS.CEP,
            concat_ws("-", 
                when(ESTABELECIMENTOS.DDD_CONTATO.isNull(), None).otherwise(ESTABELECIMENTOS.DDD_CONTATO),
                when(ESTABELECIMENTOS.TELEFONE_CONTATO.isNull(), None).otherwise(ESTABELECIMENTOS.TELEFONE_CONTATO)
            ).alias("TELEFONE"),
            ESTABELECIMENTOS.EMAIL
        )
    )

    #display(dataframe.show(5))
    return dataframe

# Define função que manipula a anterior e salva os dados em um csv poderia ser sql, mas não vamos tentar.
def salvar_df_cnae(CNAES:dict[int,str] = CNAES):
    from backup_limpeza import backup_limpeza_simples
    """
    Args:
        CNAES (dict[int,str], optional): informa um dicionário com os códigos e descrição cnae.
    Return:
        dados_pandas : salva o dataframe gerador pela função em um arquivo único csv e arquivos parquets
    """
    arquivo_csv = os.path.join(dir_dados, r"csv\BASE_RFB.csv")
    if os.path.exists(arquivo_csv):
        backup_limpeza_simples(pasta=arquivo_csv.replace(r"BASE_RFB.csv", ""), nome_zipado=f"BASE_RFB_{strftime('%d-%m-%Y %H_%M_%S', localtime())}.zip")
    dados = None
    for cod_cnae, descricao_cnae in CNAES.items():
        print(f'Filtrando: {cod_cnae}: {descricao_cnae}')
        dados = filtragem_cnae_df(cod_cnae)
        dados = dados.withColumn(
                        "CNAE_DESCRICAO", lit(descricao_cnae.upper())
                    )
        dados_pandas = dados.toPandas()
        dados_pandas.to_csv(arquivo_csv, sep=";",mode="a", encoding="utf-8", index=False)

# Executa o pipeline
salvar_df_cnae(CNAES)

