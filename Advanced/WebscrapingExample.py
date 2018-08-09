import requests
import bs4

res = requests.get('https://www.ploggingdev.com')
res.raise_for_status()

# print(res.text)
print("{} bytes".format(len(res.text)))
print("HTTP status code: {}".format(res.status_code))
print("response object type: {}".format(type(res)))

# mysite = open("mysite.html", "wb")
# print("Writing the response content to mysite.html")
#
# for chunk in res.iter_content(10000):
#     mysite.write(chunk)
#
# mysite.close()
# print("Done writing")

# crawl site from first post to latest post

print("Fetching all blog posts")

current_url = 'https://www.ploggingdev.com/2016/11/hello-world/'

urls = list()
titles = list()
keywords = list()

while True:
    print("URL list=", urls)
    print("titles list=", titles)
    print("keywords list=", keywords)
    urls.append(current_url)

    res = requests.get(current_url)
    res.raise_for_status()

    current_page = bs4.BeautifulSoup(res.text, "html.parser")
    print(current_page)
    current_title = current_page.select('title')[0].getText()
    titles.append(current_title)

    current_keywords = current_page.select('meta[name="keywords"]')[0].get('content')
    keywords.append(current_keywords)

    # url for next blog post
    try:
        current_url = current_page.select('ul[class="pager blog-pager"] > li[class="next"] > a')[0].get('href')
    except IndexError as ie:
        break

zipped = zip(range(1, len(urls) + 1), titles, urls, keywords)
print("Zipped elements = ", zipped)

for blog_num, blog_title, blog_url, blog_keywords in zipped:
    print(blog_num)
    print(blog_title)
    print(blog_url)
    print(blog_keywords)
