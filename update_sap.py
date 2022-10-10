import requests

url = "http://127.0.0.1:90/api/DoSapPaymentsMultiple"

payload={}
headers = {}

def main():
	response = requests.request("POST", url, headers=headers, data=payload)
	print(response.text)
