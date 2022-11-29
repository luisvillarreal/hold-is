query = '''
SELECT
cuentassn.banco AS `cuentassn.banco`,
cuentassn.cuenta AS `cuentassn.cuenta`,
layout.nombre AS `layout.nombre`,
pendientes_pagar.moneda AS `pendientes_pagar.moneda`,
cuentas.cuenta AS `cuentas.cuenta`,
cuentassn.bicswift AS `cuentassn.bicswift`,
pago_procesado.secuencial_diario AS `pago_procesado.secuencial_diario`,
pago_procesado.referencia_numerica AS `pago_procesado.referencia_numerica`,
pago_procesado.referencia_numerica_35 AS `pago_procesado.referencia_numerica_35`,
pago_procesado.referencia_alphanumerica AS `pago_procesado.referencia_alphanumerica`,
sn.rfc AS `sn.rfc`,
sn.zip_code AS `sn.zip_code`,
sn.country AS `sn.country`,
sn.nombre AS `sn.nombre`,
sn.address AS `sn.address`,
sn.city AS `sn.city`,
sn.codigo AS `sn.codigo`,
company.razon_social AS `company.razon_social`,
sum(pago_efectuado.importe) AS `pago_efectuado.importe`
FROM pago_efectuado
INNER JOIN pago_procesado ON pago_efectuado.id = pago_procesado.pago_efectuado_id
INNER JOIN pendientes_pagar ON pago_efectuado.pendiente_pagar_id = pendientes_pagar.id
INNER JOIN cuentas ON pago_efectuado.cuenta_id = cuentas.id
INNER JOIN layout ON pago_efectuado.layout_id = layout.id
INNER JOIN cuentassn ON pago_efectuado.cuenta_sn_id = cuentassn.id
INNER JOIN sn ON cuentassn.sn_id = sn.id
INNER JOIN company ON cuentas.company_id = company.id
WHERE pago_procesado.lambda_dt is NULL
AND layout.nombre in ('HSBC TEF','HSBC SPEI','HSBC SPID','CITIBANAMEX MB','CITIBANAMEX SPID', 'CITIBANAMEX SPEI', 'CITIBANAMEX USD')
AND pago_procesado.referencia_numerica is not NULL
AND pago_procesado.status = 0
GROUP BY
cuentassn.banco,
cuentassn.cuenta,
layout.nombre,
pendientes_pagar.moneda,
cuentas.cuenta,
pago_procesado.secuencial_diario,
pago_procesado.referencia_numerica,
pago_procesado.referencia_numerica_35,
pago_procesado.referencia_alphanumerica,
sn.rfc,
sn.zip_code,
sn.country,
sn.nombre,
sn.address,
sn.city,
sn.codigo,
company.razon_social
'''
