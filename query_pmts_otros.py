query = '''
SELECT
pendientes_pagar.empbankcode AS `pendientes_pagar.empbankcode`,
pendientes_pagar.cuenta_empleado AS `pendientes_pagar.cuenta_empleado`,
layout.nombre AS `layout.nombre`,
pendientes_pagar.moneda AS `pendientes_pagar.moneda`,
pendientes_pagar.owner AS `pendientes_pagar.owner`,
cuentas.cuenta AS `cuentas.cuenta`,
pago_procesado_emp.referencia_numerica AS `pago_procesado_emp.referencia_numerica`,
pago_procesado_emp.referencia_numerica_35 AS `pago_procesado_emp.referencia_numerica_35`,
pago_procesado_emp.referencia_alphanumerica AS `pago_procesado_emp.referencia_alphanumerica`,
company_address.zip_code AS `company_address.zip_code`,
company_address.country AS `company_address.country`,
company_address.address AS `company_address.address`,
company_address.city AS `company_address.city`,
sum(pago_efectuado.importe) AS `pago_efectuado.importe`
FROM pago_efectuado
INNER JOIN pago_procesado_emp ON pago_efectuado.id = pago_procesado_emp.pago_efectuado_id
INNER JOIN pendientes_pagar ON pago_efectuado.pendiente_pagar_id = pendientes_pagar.id
INNER JOIN cuentas ON pago_efectuado.cuenta_id = cuentas.id
INNER JOIN layout ON pago_efectuado.layout_id = layout.id
INNER JOIN company ON cuentas.company_id = company.id
INNER JOIN company_address ON company_address.id = 1
WHERE pago_procesado_emp.lambda_dt is NULL
AND layout.nombre in ('HSBC TEF','HSBC SPEI','HSBC SPID','CITIBANAMEX MB','CITIBANAMEX SPID')
AND pago_procesado_emp.referencia_numerica is not NULL
AND pago_procesado_emp.status = 0
GROUP BY
pendientes_pagar.empbankcode,
pendientes_pagar.cuenta_empleado,
layout.nombre,
pendientes_pagar.moneda,
pendientes_pagar.owner,
cuentas.cuenta,
pago_procesado_emp.referencia_numerica,
pago_procesado_emp.referencia_numerica_35,
pago_procesado_emp.referencia_alphanumerica,
company_address.zip_code,
company_address.country,
company_address.address,
company_address.city
'''
