import requests

r = requests.get('https://imgs.xkcd.com/comics/python.png')

print(r.content)

with open('comic.png', 'wb') as f:
    f.write(r.content)

print(r.status_code)
print(r.ok)
print(r.headers)


payload = {'username': 'fiona', 'password': 'password'}
r = requests.post('https://httpbin.org/post', params = payload)

print(r.text)
print(r.url)

print(r.json())

r_dict = r.json()

print(r_dict['password'])


r = requests.put('https://httpbin.org/post', params = payload)

r = requests.get('https://httpbin.org/basic-auth/corey/testing', auth = ('corey', 'testing'))

print(r.text)




for i in {1..<number of files>}; do
            wget http://s3.healthdata.org/gbd-api-2017-public/<hash of a file...>-$i.zip;
         done