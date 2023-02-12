import requests


url= ("https://app.goflightlabs.com/flights?access_key=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiI0IiwianRpIjoiNGY2ZGNmNzFmMWUzZjczN2RjOWNjNDcyMGVmMTg2ZWZiNjM5Y2E5NDM0YjhlZjg2NDRlZDliYTkyZWQ5ZGUzYTA4OTI5Mjc1NTZhY2I2MmQiLCJpYXQiOjE2NzYyMTMwOTUsIm5iZiI6MTY3NjIxMzA5NSwiZXhwIjoxNzA3NzQ5MDk1LCJzdWIiOiIyMDA0NiIsInNjb3BlcyI6W119.LiPMtKp9QVbL5boccFm9HoxATQH4Tj3GYQSE3AzFisKlwMz1VRFG4ACUryH1LDx-hkGiwRDuTLZ0KjK_-FH1_g&deplata=LHW")
response = requests.get(url)
data = response.json()

print(data)

writeFile =open('file_name.json', 'w')
writeFile.write(str(data))
writeFile.close()


