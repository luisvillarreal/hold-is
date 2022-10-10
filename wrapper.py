import time
import sys
from subprocess import run, PIPE

FREQ = 30

while(True):
  
  import send_data_aws_gastos as gastos
  import send_data_aws_otros as otros
  import update_sap as sap

  proc = run(['git', 'pull'], shell = True, stdout = PIPE, stderr = PIPE)
  print(proc.stdout.decode('utf-8'))
  print(proc.stderr.decode('utf-8'))

  try:
    print('Running Gastos...')
    gastos.main()
  except:
    print('Could not run Gastos...')
  
  #try:
  print('Running Otros...')
  otros.main()
  #except:
  #  print('Could not run Otros...')

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
