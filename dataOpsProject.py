import requests
import json
import os
import pandas as pd
import time
import pyodbc
import csv
import logging

#logger
caminho_log = 'C:/Users/Dyh/Documents/DataOps/meu_log.txt'
logging.basicConfig(filename=caminho_log, level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

#Endopoints
people = "https://swapi.dev/api/people/?"
planets = "https://swapi.dev/api/planets/?"
films = "https://swapi.dev/api/films/?"

retries = 0
#Função para fazer o request
def get_data(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            print("Datos coletados com sucesso")
        elif retries == 15:
            print("Falha ao obter dados tentando novamente")
            time.sleep(1)
            retries += 1
    except Exception as error:
        print(error)
        logging.error(f"Erro durante a operação: {error}")
    return response

#Função para salvar os dados Brutos

def salvar_raw(data, name):
    raw_path = "C:/Users/Dyh/Documents/DataOps/raw/"
    path_name = raw_path + f'{name}'

    try:
        if not os.path.exists(raw_path):
            os.makedirs(raw_path)
        with open(path_name, 'w', encoding='utf-8') as arquivo:
            json.dump(data, arquivo, indent=4, ensure_ascii=False)
        print(f'Arquivo {name} salvo com sucesso!')
    except Exception as e:
        print(f'Ocorreu um erro ao salvar o arquivo: {str(e)}')
        logging.error(f"Erro durante a operação: {e}")

#Coletando e Salvando dados RAW
#Salvando localmente
people_json = get_data(people).json()
planets_json = get_data(planets).json()
films_json = get_data(films).json()
salvar_raw(people_json, "people")
salvar_raw(planets_json, "planets")
salvar_raw(films_json, "films")

def create_df(dados):
    if dados is not None:
        personagens = dados.get("results", [])
        df = pd.DataFrame(personagens)
        return df
    else:
        return None

#Criando DF's
df_people = create_df(people_json)
df_planets = create_df(planets_json)
df_films = create_df(films_json)

#Limpando e padronizando os Dataframes

#Função para Padronizar os campos de data

def padronizar_data(data_str):
    data = pd.to_datetime(data_str)
    data_formatada = data.strftime('%Y-%m-%d')
    return data_formatada

# Aplicar a função de padronização às colunas de datas dos DF's
df_films['Criado'] = df_films['created'].apply(padronizar_data)
df_films['Editado'] = df_films['edited'].apply(padronizar_data)
df_people['Criado'] = df_people['created'].apply(padronizar_data)
df_people['Editado'] = df_people['edited'].apply(padronizar_data)
df_planets['Criado'] = df_planets['created'].apply(padronizar_data)
df_planets['Editado'] = df_planets['edited'].apply(padronizar_data)

#Dropando Coluna de data antes da padronização
#Definindo todas as colunas como String
df_people = df_people.drop(['created','edited'], axis=1).astype(str)
df_planets = df_planets.drop(['created','edited'], axis=1).astype(str)
df_films = df_films.drop(['created','edited'], axis=1).astype(str)

#Função Salvar arquivos silver(Tratados)
#Poderia usar a mesma função anterior, mas quis criar outra function

def salvar_silver(data, name):
    silver_path = "C:/Users/Dyh/Documents/DataOps/silver/"
    path_name = silver_path + f'{name}'

    try:
        if not os.path.exists(silver_path):
            os.makedirs(silver_path)
        data.to_csv(path_name, index=True)
        print(f'Arquivo {name} salvo com sucesso!')
    except Exception as e:
        print(f'Ocorreu um erro ao salvar o arquivo: {str(e)}')
        logging.error(f"Erro durante a operação: {e}")

#Salvando arquivos já tratados
salvar_silver(df_people, "silver_people")
salvar_silver(df_planets, "silver_planets")
salvar_silver(df_films, "silver_films")

#Criando uma conexão com o banco de dados SQL server

server = 'Diego'  # Substitua pelo nome do servidor SQL Server
database = 'prj'  # Substitua pelo nome do banco de dados
username = 'impacta'  # Substitua pelo nome de usuário
password = "impacta"  # Substitua pela senha

connection_string = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

try:
    # Conectar ao banco de dados
    conn = pyodbc.connect(connection_string)

    # Criar um cursor para executar consultas SQL
    cursor = conn.cursor()

#Persistindo o Dataframe em uma tabela no Banco de dados sql server
    for index,row in df_people.iterrows():
        sql = "INSERT INTO gold_prj_dataops (name, height, mass, hair_color, skin_color, eye_color, birth_year, gender, homeworld, species, vehicles, Criado, Editado) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        val = (row['name'],row['height'],row['mass'],row['hair_color'],row['skin_color'],row['eye_color'],row['birth_year'],row['gender'],row['homeworld'],row['species'],row['vehicles'],row['Criado'],row['Editado'],) 
        cursor.execute(sql, val)
        conn.commit()

except pyodbc.Error as e:
    logging.error(f"Erro durante a operação: {e}")
    print(f"Erro de conexão: {e}")

