import requests
api_key=''
base_url="https://api.openweathermap.org/data/2.5/"
endpoint="weather"

city_name="Chicago"
params={'q':city_name,'appid':api_key}
response=requests.get(base_url+endpoint,params=params)
if response.status_code == 200:

    data=response.json()
    Temperature = data['main']['temp']
    Description = data['weather'][0]['description']
    city=data['name']
    print('city:',city)
    print('Temperature:',Temperature)
    print('Description:',Description)
else:
    print("Status Code:",{response.status_code})
