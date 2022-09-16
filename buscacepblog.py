import requests
import psycopg2
import sys
import re
import time
from bs4 import BeautifulSoup

dbname = sys.argv[1]
user = 'chinchila'
host = 'localhost'
password = 'chinchila'
port = '5434'
##Construção da string da conexão ao banco.
conn_string = "dbname='{0}' user='{1}' host='{2}' port= '{3}' password='{4}'".format(dbname, user, host, port ,password)
conn = psycopg2.connect(conn_string)
## Conexão estabelecida.
cursor = conn.cursor()

def incluir_banco(banco):
    cursor.execute("INSERT INTO dif_endereco_2(logradouro, bairro, cidadeestado, cep, estado, complemento) values ("+banco+");")
    conn.commit()
    print('INSERT NA dif_endereco_2 REALIZADO')


def barra_progresso(done):
    print("\rProgress: [{0:50s}] {1:.1f}%".format('#' * int(done * 50), done * 100),end='')

def contador_progesso():
    for n in range(101):
        barra_progresso(n/100)
        time.sleep(1)


linuxbusca = sys.argv[2:]
busca = str(', '.join(linuxbusca)).replace(", ","+")
cursor.execute("DROP TABLE IF EXISTS dif_endereco_2;CREATE TABLE dif_endereco_2 (logradouro varchar,bairro varchar, cidadeestado varchar,cep varchar, estado varchar, complemento varchar);")
print('TABELA dif_endereco_2 CRIADA')
conn.commit()

for page in range(1,500):
    res = requests.get(f"https://cep.guiamais.com.br/busca/{busca}?page={page}")
    if res.status_code == 200:
         content = res.content
         soup = BeautifulSoup(content, 'html.parser')
         td = soup.findChildren(name='td')
         for l in range(0, 1000, 5):
             try:
                     logradouro = td[l]
                     bairro = td[l+1]
                     cidade = td[l+2]
                     cep = td[l+4]
                     if str(logradouro).find('<i>') > 0 :
                         completo = [str(logradouro).split('>',2)[2].split('>',1)[0].upper().replace('</A','').replace("'"," "),
                                     str(bairro).split('>', 2)[2].split('>', 1)[0].upper().replace('</A', '').replace("'"," "),
                                     str(cidade).split('>', 2)[2].split('>', 1)[0].upper().replace('</A', '').replace(',','').replace("'"," "),
                                     str(cep).split('>',2)[2].split('>',1)[0].upper().replace('</A',''),
                                     str(cidade).split('>', 2)[2].split('>', 1)[0].upper().replace('</A', '').split(',', 2)]
                         uf =  completo[4]
                         ufreplace = str(uf[0]).lstrip().replace("'"," ")
                         testegustavo = list(td[l])
                         complementologradouro = str(testegustavo[4]).replace('<i>','').replace('</i>','').upper().lstrip()
                         completo = list(completo)
                         complementologradouroreplace = str(complementologradouro).replace("'"," ")
                         bancoDados = f"'{completo[0]}','{completo[1]}','{ufreplace}','{str(completo[3]).replace('-','')}', '{str(uf[1]).lstrip()}', '{complementologradouroreplace}'"
                         incluir_banco(bancoDados)
                     else:
                        completo = [str(logradouro).split('>', 2)[2].split('>', 1)[0].upper().replace('</A', '').replace("'", " "),
                                    str(bairro).split('>', 2)[2].split('>', 1)[0].upper().replace('</A', '').replace("'", " "),
                                    str(cidade).split('>', 2)[2].split('>', 1)[0].upper().replace('</A', '').replace(',', '').replace("'", " "),
                                    str(cep).split('>', 2)[2].split('>', 1)[0].upper().replace('</A', ''),
                                    str(cidade).split('>', 2)[2].split('>', 1)[0].upper().replace('</A', '').split(',', 2)]
                        uf = completo[4]
                        ufreplace = str(uf[0]).lstrip().replace("'", " ")
                        completo = list(completo)
                        bancoDados = f"'{completo[0]}','{completo[1]}','{ufreplace}','{str(completo[3]).replace('-', '')}', '{str(uf[1]).lstrip()}', ''"
                        incluir_banco(bancoDados)
             except IndexError:
                pass
             continue
    elif res.status_code == 504:
         print('TIMEOUT: FALHA NA CONEXÃO')
         cursor.execute("DROP TABLE IF EXISTS dif_endereco_2;")
         conn.commit()
         break
    else:
         break
cursor.close()
conn.close()
print('CONEXÃO FECHADA')