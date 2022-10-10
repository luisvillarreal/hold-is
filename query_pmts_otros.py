query = '''
SELECT
pendientes_pagar.empbankcode AS `pendientes_pagar.empbankcode`,
pendientes_pagar.cuenta_empleado AS `pendientes_pagar.cuenta_empleado`,
layout.nombre AS `layout.nombre`,
pendientes_pagar.moneda AS `pendientes_pagar.moneda`,
pendientes_pagar.owner AS `pendientes_pagar.owner`,
cuentas.cuenta AS `cuentas.cuenta`,
pago_procesado.referencia_numerica AS `pago_procesado.referencia_numerica`,
pago_procesado.referencia_numerica_35 AS `pago_procesado.referencia_numerica_35`,
pago_procesado.referencia_alphanumerica AS `pago_procesado.referencia_alphanumerica`,
company_address
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
AND layout.nombre in ('HSBC TEF','HSBC SPEI','HSBC SPID','CITIBANAMEX MB','CITIBANAMEX SPID')
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
