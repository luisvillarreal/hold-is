import time
import sys

import send_data_aws_gastos as gastos
import send_data_aws_otros as otros
import update_sap as sap

FREQ = 30

while(True):

	print('Running Gastos...')
	gastos.main()
	print('Running Otros...')
	otros.main()
	print('Running SAP...')
	try:
		sap.main()
	except:
		print('SAP Update service unavailable...')

	try:
		count = 0
		print('Sleeping...')
		while(True):
			time.sleep(1)
			print(str(FREQ - count).zfill(2), end = '\r')
			count += 1
			if count > FREQ:
				break
	except KeyboardInterrupt:
		print('Good bye!')
		sys.exit(0)
