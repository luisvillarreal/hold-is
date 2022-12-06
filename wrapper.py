import time
import sys
from subprocess import run, PIPE
from importlib import reload
import send_data_aws_gastos as gastos
import send_data_aws_otros as otros
import update_sap as sap
  
FREQ = 30

while(True):
  
  proc = run(['git', 'pull'], shell = True, stdout = PIPE, stderr = PIPE)
  print(proc.stdout.decode('utf-8'))
  print(proc.stderr.decode('utf-8'))

  try:
    print('Running Gastos...')
    gastos.main()
  except:
    print('Could not run Gastos...')
  
  try:
    print('Running Otros...')
    otros.main()
  except:
    print('Could not run Otros...')

  try:
    print('Running SAP...')
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
  
  reload(gastos)
  reload(otros)
  reload(sap)
