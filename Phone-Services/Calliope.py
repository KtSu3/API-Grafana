import mysql.connector
from mysql.connector import Error
import json
import requests
import time
#-------------------------------------------------------------API---------------------------------------------------------#

url = 'API-LINK'

headers = {
    'Content-Type': 'application/json',
    'Authorization': 'AUTORIZATION',
    'Cookie': 'PHPSESSID=obcb38igi9np8gt5jpllldavg4'
}

data = {
    "company_id": "8ae3841e-e0cd-4937-a49a-50f31e4f9ed2",
    "queue": "Caixas"
}

response = requests.post(url, headers=headers, data=json.dumps(data))
data1 = response.json()


#-------------------------------------------------------------API---------------------------------------------------------#
url = 'API-LINK'

headers = {
    'Content-Type': 'application/json',
    'Authorization': 'AUTORIZATION',
    'Cookie': 'PHPSESSID=obcb38igi9np8gt5jpllldavg4'
}

data = {
    "company_id": "8ae3841e-e0cd-4937-a49a-50f31e4f9ed2",
    "queue": "Financeiro"
}

response = requests.post(url, headers=headers, data=json.dumps(data))
data2 = response.json()

#-------------------------------------------------------------API---------------------------------------------------------#
url = 'API-LINK'

headers = {
    'Content-Type': 'application/json',
    'Authorization': 'AUTORIZATION',
    'Cookie': 'PHPSESSID=obcb38igi9np8gt5jpllldavg4'
}

data = {
    "company_id": "8ae3841e-e0cd-4937-a49a-50f31e4f9ed2",
    "queue": "Logistica"
}

response = requests.post(url, headers=headers, data=json.dumps(data))
data3 = response.json()


#-------------------------------------------------------------API---------------------------------------------------------#
url = 'API-LINK'

headers = {
    'Content-Type': 'application/json',
    'Authorization': 'AUTORIZATION',
    'Cookie': 'PHPSESSID=obcb38igi9np8gt5jpllldavg4'
}

data = {
    "company_id": "8ae3841e-e0cd-4937-a49a-50f31e4f9ed2",
    "queue": "Renegociacoes"
}

response = requests.post(url, headers=headers, data=json.dumps(data))
data4 = response.json()

#-------------------------------------------------------------API---------------------------------------------------------#
url = 'API-LINK'

headers = {
    'Content-Type': 'application/json',
    'Authorization': 'AUTORIZATION',
    'Cookie': 'PHPSESSID=obcb38igi9np8gt5jpllldavg4'
}

data = {
    "company_id": "8ae3841e-e0cd-4937-a49a-50f31e4f9ed2",
    "queue": "Suporte"
}

response = requests.post(url, headers=headers, data=json.dumps(data))
data5 = response.json()

#--------------------------------------------------------CONECT DATABASE---------------------------------------------------#
def connect_db2(): 
    try:
        connection = mysql.connector.connect(
            host='IP-DB',
            user='root',
            password='PASSWD',
            database='testes'
        )
        if connection.is_connected():
            print('Sucess conection')
            insert_table(connection,data1,data2,data3,data4,data5)
            connection.close()
    except Error as e:
        print('Falha ao conectar ao banco:', e)
        return None
    
#--------------------------------------------------------CONECT DATABASE---------------------------------------------#



def insert_table(connection,data1,data2,data3,data4,data5):
    try:
        cursor = connection.cursor()

        
        cursor.execute("TRUNCATE TABLE calliope_filas;")

        
        for agent in data1['agents']:
            nome = agent['name']
            ramal = agent['extension']
            estado = agent['state']
            tempo_logado = agent['loged_time']
            atendidas_agente = agent['answered_count']
            nao_atendida = data1['inbound_canceled']
            atendidas_filas = data1['inbound_total']
            

            sql = f'''
                INSERT INTO calliope_filas (nome, fila, ramal, estado, atendidas_agente, nao_atendidas, tempo_logado, espera, atendidas_fila)
                VALUES ('{nome}', '{data1['queue']}', '{ramal}', '{estado}', '{atendidas_agente}', '{nao_atendida}', '{tempo_logado}', '{data1['calls_in_queue']}','{atendidas_filas}');
            '''
            cursor.execute(sql)

        
        for agent in data2['agents']:
            nome = agent['name']
            ramal = agent['extension']
            estado = agent['state']
            tempo_logado = agent['loged_time']
            atendidas_agente = agent['answered_count']
            nao_atendida = data2['inbound_canceled']
            atendidas_filas = data2['inbound_total']
            

            sql = f'''
                INSERT INTO calliope_filas (nome, fila, ramal, estado, atendidas_agente, nao_atendidas, tempo_logado, espera, atendidas_fila)
                VALUES ('{nome}', '{data2['queue']}', '{ramal}', '{estado}', '{atendidas_agente}', '{nao_atendida}', '{tempo_logado}', '{data2['calls_in_queue']}', '{atendidas_filas}');
            '''
            cursor.execute(sql)

        for agent in data3['agents']:
            nome = agent['name']
            ramal = agent['extension']
            estado = agent['state']
            tempo_logado = agent['loged_time']
            atendidas_agente = agent['answered_count']
            nao_atendida = data3['inbound_canceled']
            atendidas_filas = data3['inbound_total']
            

            sql = f'''
                INSERT INTO calliope_filas (nome, fila, ramal, estado, atendidas_agente, nao_atendidas, tempo_logado, espera, atendidas_fila)
                VALUES ('{nome}', '{data3['queue']}', '{ramal}', '{estado}', '{atendidas_agente}', '{nao_atendida}', '{tempo_logado}', '{data3['calls_in_queue']}', '{atendidas_filas}');
            '''
            cursor.execute(sql)

        for agent in data4['agents']:
            nome = agent['name']
            ramal = agent['extension']
            estado = agent['state']
            tempo_logado = agent['loged_time']
            atendidas_agente = agent['answered_count']
            nao_atendida = data4['inbound_canceled']
            atendidas_filas = data4['inbound_total']

            sql = f'''
                INSERT INTO calliope_filas (nome, fila, ramal, estado, atendidas_agente, nao_atendidas, tempo_logado, espera, atendidas_fila)
                VALUES ('{nome}', '{data4['queue']}', '{ramal}', '{estado}', '{atendidas_agente}', '{nao_atendida}', '{tempo_logado}', '{data4['calls_in_queue']}', '{atendidas_filas}');
            '''
            cursor.execute(sql)

        for agent in data5['agents']:
            nome = agent['name']
            ramal = agent['extension']
            estado = agent['state']
            tempo_logado = agent['loged_time']
            atendidas_agente = agent['answered_count']
            nao_atendida = data5['inbound_canceled']
            atendidas_filas = data5['inbound_total']
            

            sql = f'''
                INSERT INTO calliope_filas (nome, fila, ramal, estado, atendidas_agente, nao_atendidas, tempo_logado, espera, atendidas_fila)
                VALUES ('{nome}', '{data5['queue']}', '{ramal}', '{estado}', '{atendidas_agente}', '{nao_atendida}', '{tempo_logado}', '{data5['calls_in_queue']}', '{atendidas_filas}');
            '''
            cursor.execute(sql)

    

        connection.commit()
        connection.close()
        print("Success insert data.")
    except Error as e:
        print("Error insert data:", e)
        connection.rollback()

