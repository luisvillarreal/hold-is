from configparser import ConfigParser
import json
import os
import sys
from datetime import datetime, timedelta
from decimal import Decimal
import time
import pymysql
import boto3

import query_pmts_gastos
import cta_fuente

query_pmts = query_pmts_gastos.query
cuenta_fuente = cta_fuente.cuenta_fuente
cuenta_fuente_alt = cta_fuente.cuenta_fuente_alt

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

def clean_reference(conn):

    cur = conn.cursor()
    query = '''
    UPDATE pago_procesado
    SET referencia_alphanumerica = NULL, secuencial_diario = NULL
    '''

    cur.execute(query)
    conn.commit()


def generate_reference(conn):
    cur = conn.cursor()
    query = '''
    SELECT DISTINCT cuentassn.id AS `cuentassn.id`, pendientes_pagar.moneda AS `pendientes_pagar.moneda`,
    pago_efectuado.layout_id AS `pago_efectuado.layout_id`
    FROM pago_procesado
    INNER JOIN pago_efectuado ON pago_procesado.pago_efectuado_id = pago_efectuado.id
    INNER JOIN cuentassn ON cuentassn.id = pago_efectuado.cuenta_sn_id
    INNER JOIN pendientes_pagar ON pendientes_pagar.id = pago_efectuado.pendiente_pagar_id
    WHERE pago_procesado.referencia_alphanumerica is NULL
    '''

    cur.execute(query)

    query_update = '''
    UPDATE pago_procesado
    INNER JOIN pago_efectuado ON pago_procesado.pago_efectuado_id = pago_efectuado.id
    INNER JOIN cuentassn ON cuentassn.id = pago_efectuado.cuenta_sn_id
    INNER JOIN pendientes_pagar ON pendientes_pagar.id = pago_efectuado.pendiente_pagar_id
    SET pago_procesado.referencia_alphanumerica = %s, pago_procesado.referencia_numerica = %s
    WHERE cuentassn.id = %s
    AND pendientes_pagar.moneda = %s
    AND pago_efectuado.layout_id = %s
    AND pago_procesado.referencia_alphanumerica is NULL
    '''

    update_values = []
    for item in cur.fetchall():
        reference_number = int(str(time.time()).replace('.','')[:-2])
        reference_alphanumber = hex(reference_number).upper().lstrip('0X')
        update_values.append(
            (
                reference_alphanumber,
                reference_number,
                item['cuentassn.id'],
                item['pendientes_pagar.moneda'],
                item['pago_efectuado.layout_id']
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
    SELECT DISTINCT pendientes_pagar.moneda AS `pendientes_pagar.moneda`,
    pago_efectuado.layout_id AS `pago_efectuado.layout_id`
    FROM pago_procesado
    INNER JOIN pago_efectuado ON pago_procesado.pago_efectuado_id = pago_efectuado.id
    INNER JOIN pendientes_pagar ON pendientes_pagar.id = pago_efectuado.pendiente_pagar_id
    WHERE pago_procesado.referencia_numerica_35 is NULL
    '''

    cur.execute(query)

    query_update = '''
    UPDATE pago_procesado
    INNER JOIN pago_efectuado ON pago_procesado.pago_efectuado_id = pago_efectuado.id
    INNER JOIN pendientes_pagar ON pendientes_pagar.id = pago_efectuado.pendiente_pagar_id
    SET pago_procesado.referencia_numerica_35 = %s
    WHERE pendientes_pagar.moneda = %s
    AND pago_efectuado.layout_id = %s
    AND pago_procesado.referencia_numerica_35 is NULL
    '''

    update_values = []
    for item in cur.fetchall():
        reference_number = hex(int(str(time.time()).replace('.',''))).upper().lstrip('0X')
        update_values.append(
            (
                reference_number,
                item['pendientes_pagar.moneda'],
                item['pago_efectuado.layout_id']
            )
        )
        time.sleep(0.5)

    conn.commit()

    if len(update_values) > 0:
        tmstp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f'{tmstp} Generando referencias numericas 35...')

    cur.executemany(query_update, update_values)
    conn.commit()

def generate_sequentials(conn):

    cur = conn.cursor()
    query = '''
        SELECT DATE(pago_procesado.date_created) AS dt, pago_efectuado.layout_id AS lyt,
        MAX(pago_procesado.secuencial_diario) AS seq
        FROM pago_procesado
        INNER JOIN pago_efectuado ON pago_efectuado.id = pago_procesado.pago_efectuado_id
        GROUP BY dt, lyt
        HAVING DATEDIFF(CURRENT_DATE(), dt) < 30;
    '''
    cur.execute(query)
    date_seq_lookup = {
        f"{item['dt'].strftime('%Y%m%d')}-{item['lyt']}": item['seq'] if item['seq'] else 1  for item in cur.fetchall()
    }

    conn.commit()

    query = '''
        SELECT DISTINCT pago_procesado.referencia_alphanumerica AS ref_num, DATE(pago_procesado.date_created) AS dt,
        pago_efectuado.layout_id AS lyt
        FROM pago_procesado
        INNER JOIN pago_efectuado ON pago_efectuado.id = pago_procesado.pago_efectuado_id
        WHERE pago_procesado.secuencial_diario is NULL
    '''

    cur.execute(query)

    update_values = []
    for item in cur.fetchall():
        key = f"{item['dt'].strftime('%Y%m%d')}-{item['lyt']}"
        date_seq_lookup[key] += 1
        rank = date_seq_lookup[key]
        update_values.append((rank, item['ref_num'], item['dt']))

    conn.commit()

    query = '''
    UPDATE pago_procesado
    SET secuencial_diario = %s
    WHERE referencia_alphanumerica = %s
    AND DATE(date_created) = %s
    AND secuencial_diario is NULL
    '''

    if len(update_values) > 0:
        tmstp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f'{tmstp} Generando secuenciales...')
    cur.executemany(query, update_values)
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
                    'HSBC SPID': [],
                    'CITIBANAMEX MB':[],
                    'CITIBANAMEX SPID': [],
                    'CITIBANAMEX SPEI': [],
                    'CITIBANAMEX USD': []
                },
                'USD': {
                    'HSBC TEF': [],
                    'HSBC SPEI': [],
                    'HSBC SPID': [],
                    'CITIBANAMEX MB':[],
                    'CITIBANAMEX SPID': [],
                    'CITIBANAMEX SPEI': [],
                    'CITIBANAMEX USD': []
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
                try:
                    dict_[i]['codigo_banco'],  dict_[i]['banco.nombre'], dict_[i]['banco.codigo_swift'] = get_bank_info(dict_[i]['cuentassn.banco'], layout_name, conn)
                except:
                    print(f"No se encontro {dict_[i]['cuentassn.banco']} en la tabla de Bancos...")
                    continue

                dict_[i]['vencimiento_date'] = (datetime.now() + timedelta(days = 7)).strftime('%Y-%m-%d %H:%M:%S.0') if currency == 'MXN' else ' '

                if 'banamex' in layout_name.lower():
                    tipo_cuenta = '05'
                    codigo_transaccion = '001'

                    if len(dict_[i]['cuentassn.cuenta']) != 18:
                        print(f"Transaccion rechazada, tabla: pago_procesado, id: {dict_[i]['pago_procesado.id']}")
                        reject_tranx(
                            dict_[i]['pago_procesado.id'],
                            'pago_procesado',
                            'Cuenta beneficiario no tiene 18 digitos (CLABE).',
                            2,
                            conn
                        )
                        continue

                    if dict_[i]['codigo_banco'] == '000':
                        codigo_transaccion = '072'


                    dict_[i]['tipo_cuenta_beneficiario'] =  tipo_cuenta
                    dict_[i]['codigo_transaccion'] = codigo_transaccion

                    codigo_transaccion_local = '05'
                    dict_[i]['cuentas.cuenta'] = cuenta_fuente
                    if currency == 'USD' and 'banamex' not in dict_[i]['banco.nombre'].lower():
                        dict_[i]['cuentas.cuenta'] = cuenta_fuente_alt                        
                        codigo_transaccion_local = '11'
                    dict_[i]['codigo_transaccion_local'] = codigo_transaccion_local



                elif 'hsbc' in layout_name.lower():

                    if len(dict_[i]['cuentassn.cuenta']) != 18:
                        print(f"Transaccion rechazada, tabla: pago_procesado, referencia_alphanumerica: {dict_[i]['pago_procesado.referencia_alphanumerica']}")
                        reject_tranx(
                            'referencia_alphanumerica',
                            dict_[i]['pago_procesado.referencia_alphanumerica'],
                            'pago_procesado',
                            'Cuenta beneficiario no tiene 18 digitos (CLABE).',
                            2,
                            conn
                        )
                        continue
                        
                    dict_[i]['ref_cuenta_dbt_mas_rfc_crt'] = f"{dict_[i]['cuentas.cuenta'].strip()[:7]}"
                    print(dict_[i]['ref_cuenta_dbt_mas_rfc_crt'])

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
                                update_dt_mysql(conn, payload, 'pago_procesado.referencia_alphanumerica', 'pago_procesado', 'lambda_dt')
                                update_dt_mysql(conn, json.loads(json_response['body']), 'pago_procesado.referencia_alphanumerica', 'pago_procesado', 'layout_dt')
                                update_dt_mysql(conn, payload, 'pago_procesado.referencia_alphanumerica', 'pago_procesado', 'bank_response_dt')
                                update_mysql(conn, payload, 'pago_procesado.referencia_alphanumerica', 'pago_procesado', 'status', 1)
                            except KeyError:
                                tmstp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                print(f'{tmstp} Could not update table PAGO_PROCESADO. Lambda Function returned nothing.')

        conn.close()
