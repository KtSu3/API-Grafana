import mysql.connector
from mysql.connector import Error
import json
import requests
import time
#-------------------------------------------------------------API---------------------------------------------------------#

url = "API-LINK"


print(url)
payload = json.dumps({
  "filter": {
"status": "EA",
},

})
headers = {
  'Authorization': 'AUTORIZATION',
  'Content-Type': 'application/json'
}

response = requests.request("GET", url, headers=headers, data=payload)
data = response.json()
print(data)

#-------------------------------------------------------------API---------------------------------------------------------#

url = "API-LINK"

payload= json.dumps({
    
  "options": {
    "limit": 1000
  }

})

headers = {
  'Authorization': 'AUTORIZATION',
  'Content-Type': 'application/json'
}

response = requests.request("GET", url, headers=headers, data=payload)
data2 = response.json()
print(data2)
#-------------------------------------------------------------API---------------------------------------------------------#

#--------------------------------------------------------CONECT DATABASE---------------------------------------------------#
def connect_db1(): 
    try:
        connection = mysql.connector.connect(
            host='IP-DB',
            user='root',
            password='PASSWD',
            database='atendimentos_opa'
        )
        if connection.is_connected():
            print('Sucess conection')
            insert_table(connection, data, data2) 
            connection.close()
    except Error as e:
        print('Falha ao conectar ao banco:', e)
        return None
#--------------------------------------------------------CONECT DATABASE---------------------------------------------#


#ID TO NAME
id_to_department = {
   '5bf73d1d186f7d2b0d647a60': 'Comercial',
    '658d6ac2a6233e2fc6e16ee2': 'Auxilio de campo Configuracoes',
    '5d1624085e74a002308aa25e': 'Financeiro',
    '64b015212eafcbb48bfe0621': 'Noc',
    '652da3d79717665daea8ea15': 'Projeto Tecnicos',
    '658d74d5fc89ce8dc6fad6e9': 'Provisionamento de Equipamento Tecnicos',
    '5d1629315e74a002308aa262': 'Renegociacoes',
    '64c2cce9c1131b4e08cefe92': 'Retencao de Clientes',
    '6536d9fe9717665daebae3e6': 'Satisfacao',
    '5bf73d1d186f7d2b0d647a61': 'Suporte',
    '65157426150fbd707a0af8a1': 'Suporte manutencao interna',
    '65c0d3c85ead47c9b1b4353d': 'Telefonia Tecnicos',
    '5d1623f35e74a002308aa25d': 'Agendamento',
    '6603233d411ea3e1afe88a54': 'Suporte atendimento aos tecnicos',
    '5bf73d1d186f7d2b0d647a64': 'Agente virtual',
}

id_to_agent = {
    '6511eb1b2eafcbb48bfee7c3': 'Alex comercial', #OK
    '659be3f9a6233e2fc6ef9ec4': 'Alessandra Comercial', #OK
    '65d606bdfc89ce8dc65e86dc': 'Alessandro Retenção', #OK
    '65eb51df19884447a0922a88': 'Alexsandra Retencao', #OK
    '64d51534077494074fa8577a': 'Ana Comercial', #OK
    '65d49b105ead47c9b101e4a3': 'Andrey Suporte', #OK
    '64d52767077494074fa85d27': 'Danielly Comercial', #OK
    '64d51620077494074fa85856': 'Brenda Renegociacões', #OK
    '64d516482eafcbb48bfe74a0': 'Bruno Suporte', #OK
    '653bb3019717665daec4f98a': 'Camila RH', #OK
    '64d5168d2eafcbb48bfe74d1': 'Cassio Comercial', #OK
    '65f9958019884447a0a2a101': 'Cleide Comercial', #OK
    '65770b3980f205f2d5b4beee': 'Daiana Agendamento', #OK
    '652da4ae9717665daea8eee1': 'Daniel Projeto', #OK
    '64d51db12eafcbb48bfe755a': 'Daniele Retencao', #OK
    '64d51de2077494074fa85981': 'Daniely Agendamento', #OK
    '652da3709717665daea8e753': 'Daysi Bittencourt', #OK
    '65f21a8e19884447a09bb553': 'Debora Comercial', #OK
    '64d51491077494074fa856e7': 'Devair setor Torre de controle', #OK
    '652142d59717665dae984c44': 'Erica G Nascimento', #OK
    '660c27f09b9682e37909acf2': 'Fabiana Retençao', #OK
    '654f6e2749f8730d417a2404': 'Felipe Mateus Floriani', #OK
    '65d73375cb2044785ea416c1': 'Flavia Suporte', #OK
    '64d51e37077494074fa859fc': 'Francini Financeiro', #OK 
    '65255a1c9717665dae9ca45d': 'Gabrieli Comercial', #OK
    '64d51e832eafcbb48bfe7650': 'Gabriella Comercial', #OK
    '652438879717665dae9ac02f': 'Gladis Financeiro', #OK
    '64d52205077494074fa85c21': 'Igor Suporte', #OK
    '64fb0d772eafcbb48bfed13f': 'Integracao IXC-OPA', #OK
    '64d520bb077494074fa85af2': 'Jannaina Comercial', #OK
    '64d5280a077494074fa85da7': 'Jessica Financeiro', #OK
    '65366e808ecdccbdf77f7c07': 'Jhoni', #OK
    '5d1642ad4b16a50312cc8f4d': 'Joy', #OK
    '65a16f8db185884c19452d64': 'João NOC', #OK
    '64b014da2eafcbb48bfe05ea': 'Kauã NOC',   #OK
    '65149ca0490b17f59ddb750b': 'Killian Projeto', #OK
    '65ef00ac411ea3e1afd06f69': 'Leonardo NOC',  #OK
    '652da4469717665daea8ecef': 'Luan Projeto', #OK
    '64d520162eafcbb48bfe76cb': 'Luan Gestao', #OK
    '64d5214f077494074fa85b2b': 'Luan Suporte', #OK
    '64d527c9077494074fa85d68': 'Lucas Suporte',  #OK
    '65de363590ecc39697e39d2c': 'Yasmin Comercial', #OK
    '6560913949f8730d41b14011': 'Valeria Comercial', #OK
    '65d8af7a7c1c3678efd46572': 'Wellinton Agendamento', #OK
    '64d521972eafcbb48bfe7782': 'Tiago Suporte', #OK
    '64b016632eafcbb48bfe07b2': 'Thiago NOC', #OK
    '6512d175490b17f59d1be3d2': 'Thiago Bento da Costa', #OK
    '659ed10ca6233e2fc6f577a2': 'Thaina Validacao', #OK
    '64d521b8077494074fa85ba6': 'Tayane Comercial', #OK
    '65faf863a480317de308ef22': 'Teste1', #OK
    '65faf8d0a480317de308f44e': 'Teste2', #OK
    '65faf8e3a480317de308f51a': 'Teste3', #OK
    '65faf8f2a480317de308f577': 'Teste 4', #OK
    '65faf9dc795187f9de285a16': 'Teste 438u43', #OK
    '65faf90d795187f9de28427d': 'Teste 5', #OK
    '65faf92219884447a0a50e12': 'Teste 6', #OK
    '65faf94b19884447a0a50f6a':  'Teste 7', #OK
    '65faf95b411ea3e1afdfb614': 'Teste 8', #OK
    '65faf993a480317de308fbaf': 'Teste Leonardo',#OK
    '65fc8f5b795187f9de35cf45': 'Vago', #OK
    '658d8d565ead47c9b10e3f4a': 'Yuka Técnicos', #OK
    '64c02e222eafcbb48bfe1884': 'Yuri', #OK 
    '660181b4a480317de30f30b8': 'Devs_api', #OK
    '6602c85e411ea3e1afe7c95d': 'vago', #OK
    '6602c85e795187f9de45df6a': 'vago', #OK
    '6602c86119884447a0acef9a': 'vago', #OK
    '6512be52ce33dbf6d8667ad7': 'vago_5', #OK
    '64d51e132eafcbb48bfe75d5': 'Tauana Financeiro', #OK
    '64d521e42eafcbb48bfe77fd': 'Sergio Gestor Financeiro', #OK
    '6602c86119884447a0acef9c': 'Rodrigo Agendamento', #OK
    '652da4803321422b5568e18c': 'Richard Projeto', #OK
    '64d51fe8077494074fa85a77': 'Ricardo Projeto', #OK
    '65de0f7d90ecc39697e2ee10': 'Renata Comercial', #OK 
    '6533b6399717665daeb5ebd8': 'Rafaela  Renegociacões', #OK
    '6554f66249f8730d418d4c59':'Rafaela Renegociacões', #OK
    '64d515ca2eafcbb48bfe7425': 'Priscila Comercial', #OK
    '653bbbac9717665daec531cc': 'Pedro Suporte', #OK
    '6512c947490b17f59d0f20c1': 'Nicolas NOC', #OK
    '5d1642434b16a50312cc8f43': 'Miguel NOC', #OK 
    '64d522242eafcbb48bfe7878': 'Nikolas Suporte', #OK 
    '65f1e362411ea3e1afd65e3e': 'Michelle Agendamento', #OK
    '65e22a06a480317de3e8dbf8': 'Metro2 Suporte', #OK
    '65e229d9411ea3e1afc1a373': 'Metro Suporte', #OK
    '65e8caac19884447a08e0429': 'Metro Suporte', #OK
    '64d515a22eafcbb48bfe73ec': 'Max Suporte',#OK
    '64d5271a2eafcbb48bfe78b9': 'Maria Comercial', #OK
    '64d52743077494074fa85cf6': 'Matheus Godoy Suporte', #OK
    '6511ebd12eafcbb48bfee808': 'Mateus Lopes Suporte', #OK
    '65fdd9f8795187f9de3a34f2': 'Marines Renegociações', #OK
    '6511ed92cc09b004dcd077b2': 'Maria Comercial', #OK
    '65e2180c795187f9debf3a97': 'Maik Suporte', #OK
    '6511ed92cc09b004dcd077b2': 'Maicon NOC', #OK
    '65380aea49f8730d410d5487': 'Luma Comercial', #OK
    '660d629b3196390224d322e8': 'Luiz Suporte', #OK
    '66182619a823437ea62fda14': 'Debora Silva Comercial',
    '6617ed0ced7a7a9d1fdeb938': 'Stefane Operacional',
    '661a7771ed7a7a9d1fe1903d':'Alessandro Comercial',
    '6618243ded7a7a9d1fdf2569':'Daiane Comercial',
    '661a7771ed7a7a9d1fe1903d':'Alessandro Comercial',
    '661a7b737326240825aec60f': 'Marcelo Validação',
    '6616dab7f27b4502c8159f11': 'Monalisa Renegociacoes',
    '663a8266b92b2c249d544003': 'Gabriel Financeiro',
    '6650d02e4d4413a242978aa1': 'Luiz SAC',
    '6655e9815ef7f9392cd0aaa2': 'Raissa Financeiro'
    
}


def map_department(department_id):
    return id_to_department.get(department_id)

def map_atendant(attendant_id):
    return id_to_agent.get(attendant_id)

def insert_table(connection, data, data2):
    try:
        cursor = connection.cursor()
       
        cursor.execute("TRUNCATE TABLE em_atendimento;")


        for item in data["data"]:
            protocol = item['protocolo']
            department_id = item['setor']
            client = item['id_cliente']
            attendant_id = item['id_atendente']
            department = map_department(department_id)
            attendant = map_atendant(attendant_id)
            service_start_date = item['date']
            s = service_start_date.split('T')[0] + ' ' + service_start_date.split('T')[1].split('.')[0]
            sql = f'''
                INSERT INTO em_atendimento (protocol, department, attendant, client, service_start_date)
                VALUES ('{protocol}', '{department}', '{attendant}', '{client}', DATE_SUB('{s}', INTERVAL 3 HOUR));
            '''
            cursor.execute(sql)

       
        connection.commit()
        connection.close()
        print("Success insert data 2.")
    except Error as e:
        print("Error insert data:", e)
        connection.rollback()


