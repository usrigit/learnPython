import urllib
from http.cookiejar import CookieJar

cj = CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))

login_data = urllib.parse.urlencode({'Email' : 'sribala.2001@gmail.com', 'password' : 'Sri@1986'}).encode("utf-8")

response = urllib.request.urlopen('https://www.investello.com/Account/Login/', login_data)
print(response.readlines())