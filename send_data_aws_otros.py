from configparser import ConfigParser
import json
import os
import sys
from datetime import datetime, timedelta
from decimal import Decimal
import time
import pymysql
import boto3

import query_pmts_otros

query_pmts = query_pmts_otros.query

config = ConfigParser()
config.read('config.ini')

def update_dt_mysql(conn, payload, payload_item, table_name, column):
    cursor = conn.cursor()
    query = f'''
UPDATE {table_name} SET {column} = %s WHERE {payload_item.split('.')[-1]} = %s
'''
    updated_values = []
    now = datetime.now()
    for row in payload:
        updated_values.append((now,row[payload_item]))
    cursor.executemany(query, updated_values)
    conn.commit()
    tmstp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'{tmstp} Datetime updated for {column}')

def update_mysql(conn, payload, payload_item, table_name, column, value):
    cursor = conn.cursor()
    query = f'''
UPDATE {table_name} SET {column} = %s WHERE {payload_item.split('.')[-1]} = %s
'''
    updated_values = []
    for row in payload:
        try:
            updated_values.append((value,row[payload_item]))
        except KeyError:
            tmstp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f'{tmstp} Wrong json key {payload_item}')

    if updated_values:
        cursor.executemany(query, updated_values)
        conn.commit()
        tmstp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f'{tmstp} Updated values for {column}')

def clean_reference(conn, table_name):

    cur = conn.cursor()
    query = f'''
    UPDATE {table_name}
    SET referencia_alphanumerica = NULL, secuencial_diario = NULL
    '''

    cur.execute(query)
    conn.commit()


def generate_reference(conn):
    cur = conn.cursor()
    query = '''
    SELECT id
    FROM pago_procesado_emp
    WHERE referencia_alphanumerica is NULL
    '''

    cur.execute(query)

    query_update = '''
    UPDATE pago_procesado_emp
    SET pago_procesado.referencia_alphanumerica = %s, pago_procesado.referencia_numerica = %s
    WHERE id = %s
    AND referencia_alphanumerica is NULL
    '''

    update_values = []
    for item in cur.fetchall():
        reference_number = int(str(time.time()).replace('.','')[:-2])
        reference_alphanumber = hex(reference_number).upper().lstrip('0X')
        update_values.append(
            (
                reference_alphanumber,
                reference_number,
                item['id']
            )
        )
        time.sleep(0.5)

    conn.commit()

    if len(update_values) > 0:
        tmstp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f'{tmstp} Generando referencias numericas...')

    cur.executemany(query_update, update_values)
    conn.commit()

def generate_reference_35(conn):

    cur = conn.cursor()
    query = '''
    SELECT id
    FROM pago_procesado_emp
    WHERE referencia_numerica_35 is NULL
    '''

    cur.execute(query)

    query_update = '''
    UPDATE pago_procesado_emp
    SET referencia_numerica_35 = %s
    WHERE id = %s
    AND referencia_numerica_35 is NULL
    '''

    update_values = []
    for item in cur.fetchall():
        reference_number = hex(int(str(time.time()).replace('.',''))).upper().lstrip('0X')
        update_values.append(
            (
                reference_number,
                item['id']
            )
        )
        time.sleep(0.5)

    conn.commit()

    if len(update_values) > 0:
        tmstp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f'{tmstp} Generando referencias numericas 35...')

    cur.executemany(query_update, update_values)
    conn.commit()


def reject_tranx(col_name, id, table, reason, status, conn):

    cur = conn.cursor()

    query = f'''
    UPDATE {table}
    SET comment = '{reason}',
    status = {status}
    WHERE {col_name} = '{id}'
    '''

    #print(f'Rejecting tranx: {id}; Query:{query}')

    cur.execute(query)
    conn.commit()

def get_bank_info(bank_name, source_bank, conn):

    cur = conn.cursor()

    if 'banamex' in source_bank.lower():
        column = 'codigobanamex'
    else:
        column = 'codigohsbc'

    query = f'''
SELECT {column}, nombre, codigoswift
FROM cignuzco_insumosnotas.banco
WHERE bancosap = '{bank_name}'
AND {column} is not NULL
having max(id);
'''
    #print(query)
    cur.execute(query)
    results = cur.fetchall()[0]
    codigo_banco, nombre, codigoswift = results[column], results['nombre'], results['codigoswift']

    if 'banamex' in source_bank.lower() and codigo_banco == '002':
        codigo_banco = '000'

    conn.commit()
    return codigo_banco, nombre, codigoswift

session = boto3.Session(
    aws_access_key_id = config['aws']['access_id'],
    aws_secret_access_key = config['aws']['secret_key'],
    region_name = 'us-west-2'
)


def main():
    try:
        conn = pymysql.connect(
            host = config['mysql']['host'],
            user = config['mysql']['username'],
            passwd = config['mysql']['password'],
            db = config['mysql']['db_name'],
            connect_timeout = 5,
            cursorclass = pymysql.cursors.DictCursor
        )

    except:
        tmstp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f'{tmstp} Connection to MySQL failed, exiting...')
    else:

        generate_reference(conn)
        generate_reference_35(conn)
        generate_sequentials(conn)

        tmstp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f'{tmstp} Buscando registros nuevos...')
        cur = conn.cursor()
        cur.execute(query_pmts)
        conn.commit()

        dict_ = list(cur.fetchall())
        if len(dict_) > 0:
            tmstp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f'{tmstp} Se encontro(aron) {len(dict_)} registro(s)...')

            payloads = {
                'MXN': {
                    'HSBC TEF': [],
                    'HSBC SPEI': [],
                    'HSBC SPID': []
                },
                'USD': {
                    'HSBC TEF': [],
                    'HSBC SPEI': [],
                    'HSBC SPID': []
                }
            }

            elems_to_pop = []
            for i, elem in enumerate(dict_):
                for k,v in elem.items():
                    if isinstance(v, datetime):
                        dict_[i][k] = v.strftime('%Y-%m-%dT%H:%M:%S')
                    elif isinstance(v, bytes):
                        dict_[i][k] = bool(list(v)[0])
                    elif isinstance(v, Decimal):
                        dict_[i][k] = float(v)
                layout_name = dict_[i]['layout.nombre']
                currency = dict_[i]['pendientes_pagar.moneda']
                if currency == 'MXP':
                    currency = 'MXN'
                    dict_[i]['pendientes_pagar.moneda'] = currency
                
                dict_[i]['vencimiento_date'] = (datetime.now() + timedelta(days = 7)).strftime('%Y-%m-%d %H:%M:%S.0') if currency == 'MXN' else ' '              


                if 'hsbc' in layout_name.lower():

                    if len(dict_[i]['pendientes_pagar.cuenta_empleado']) not in [11,18]:
                        print(f"Transaccion rechazada, tabla: pago_procesado_emp, referencia_alphanumerica: {dict_[i]['pago_procesado_emp.referencia_alphanumerica']}")
                        reject_tranx(
                            'referencia_alphanumerica',
                            dict_[i]['pago_procesado_emp.referencia_alphanumerica'],
                            'pago_procesado_emp',
                            'Cuenta beneficiario no cumple con la cantidad de digitos.',
                            2,
                            conn
                        )
                        continue

                try:
                    payloads[currency][layout_name].append(dict_[i])
                except:
                    pass

            lambda_client = session.client('lambda')
            for currency, layouts in payloads.items():
                for layout_name, payload in layouts.items():
                    lambda_payload = json.dumps(payload, indent = 4)
                    if payload:
                        time.sleep(2)

                        response = lambda_client.invoke(
                            FunctionName = 'paymentProcessor',
                            InvocationType = 'RequestResponse',
                            Payload = lambda_payload
                        )
                        if response['StatusCode'] == 200:
                            json_response = json.loads(response['Payload'].read())
                            try:
                                update_dt_mysql(conn, payload, 'pago_procesado_emp.referencia_alphanumerica', 'pago_procesado_emp', 'lambda_dt')
                                update_dt_mysql(conn, json.loads(json_response['body']), 'pago_procesado_emp.referencia_alphanumerica', 'pago_procesado_emp', 'layout_dt')
                                update_dt_mysql(conn, payload, 'pago_procesado_emp.referencia_alphanumerica', 'pago_procesado_emp', 'bank_response_dt')
                                update_mysql(conn, payload, 'pago_procesado_emp.referencia_alphanumerica', 'pago_procesado_emp', 'status', 1)
                            except KeyError:
                                tmstp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                print(f'{tmstp} Could not update table PAGO_PROCESADO_EMP. Lambda Function returned nothing.')

        conn.close()
