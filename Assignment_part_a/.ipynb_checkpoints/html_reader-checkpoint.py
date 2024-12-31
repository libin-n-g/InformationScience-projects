import requests
from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
import time
import urllib.parse
from selenium.webdriver.common.keys import Keys

HEADERS ={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0", "Accept-Encoding":"gzip, deflate", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "DNT":"1","Connection":"close", "Upgrade-Insecure-Requests":"1"}
URL_BASE = "https://in.indeed.com"

def get_html(url, driver):
    driver.get(url)
    time.sleep(3)
    html = driver.execute_script("return document.documentElement.outerHTML")
    return html

def get_current_url(url, job_title, location, job_type="fulltime", start=0):
    current_url = url + "jobs?q=" + urllib.parse.quote(job_title) + "&l=" + urllib.parse.quote(location) + "&limit=50&filter=0"
    if start != 0:
        current_url = current_url + "&start=" + str(start)
    return current_url

def get_current_url_with_start(current_url, start=0):
    if start != 0:
        current_url = current_url + "&start=" + str(start)
    return current_url
# html, driver = get_current_url('https://in.indeed.com/','Data Scientist',"Bengaluru")

SKILLS = [
    "Data Science", "Statistics", "Mathematics", "Computer Science", "Engineering",
    "Artificial Intelligence", "Machine Learning", "Natural Language Processing",
    "text analytics", "sentiment analysis", "deep learning", "classification",
    "pattern recognition", "Microsoft CNTK", "scikit-learn", "Keras", "Caffe", "Gluon",
    "Torch", "SageMaker", "Azure AI", "Hadoop", "NoSQL", "MapReduce", "Amazon Web Services", "Pandas", "SQL", "Seaborn",
    "MATLAB", "Web development","Spark", "Pig", "Hive", "TensorFlow", 
]
import concurrent.futures

def get_page_per_job_details(args):
    href, languages, job_types = args
    driver = webdriver.Chrome()
    filters = {}
    html = get_html(href, driver)
    content = BeautifulSoup(html, 'lxml')
    desc = content.find('div', class_='jobsearch-jobDescriptionText')
    if desc is None:
        desc = content.find('div', class_='job-info-row')
    rating_header = content.select(".jobsearch-CompanyInfoWithReview > div > div > div")
    filters["rating"] = None
    if len(rating_header) > 0:
        rating_header = rating_header[0]
        rating_element = rating_header.find_all('div')
        if len(rating_element) > 1:
            rating_element = rating_element[1]
            rating_element = rating_element.select("div[aria-label]")
            for rating_text in rating_element:
                if "stars" in rating_text["aria-label"]:
                    filters["rating"] = float(rating_text["aria-label"].strip().split(" ")[0])
                    break
        
    if desc is None:
        desc = content
    text = desc.get_text()
    text_upper = text.upper()
    # for lang in languages:
    #     if lang.upper() in text_upper:
    #         filters[lang] = True
    #     else:
    #         filters[lang] = False
    for jt in job_types:
        if jt.upper() in text_upper:
            filters[jt] = True
        else:
            filters[jt] = False
    filters["Description"] = text
    return filters

def scrape_job_details(content, filters={}, languages=[], job_types=[]):
    jobs_list = []
    html_refs = []
    for post in content.select('.job_seen_beacon'):
        # print(post.select('[data-testid="attribute_snippet_testid"]'))
        metadata = []
        for meta in post.select('[data-testid="attribute_snippet_testid"]'):
            metadata.append(meta.get_text().strip())
        refs = post.select('.jcs-JobTitle')
        if len(refs) != 1:
            print("Warning: multiple jcs-JobTitle found")
        # print(post.select('[data-testid="timing-attribute"]')[0].get_text().strip())
        data = {}
        
        try:
            data["job_title"] = post.select('.jobTitle')[0].get_text().strip()
            data["company"] = post.select('[data-testid="company-name"]')[0].get_text().strip()
            rating = post.select('[data-testid="holistic-rating"]')
            if rating:
                data["rating"] = rating[0].get_text().strip()
            location = post.select('[data-testid="text-location"]')
            if location:
                data["location"] = location[0].get_text().strip()
            date = post.select('.date')
            if date:
                data["date"] = date[0].get_text().strip()
            job_desc = post.select('.job-snippet')
            if job_desc:
                data["job_desc"] = job_desc[0].get_text().strip()
            if metadata:
                data["metadata"] = ",".join(metadata)
            data["href"] = "https://in.indeed.com" + refs[0].get('href')
            html_refs.append(data["href"])
        except IndexError:
            continue
        data.update(filters)
        jobs_list.append(data)
    args = ((b, languages, job_types) for b in html_refs)
    
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = executor.map(get_page_per_job_details, args)
        j = 0
        for future in futures:
            try:
                data = future.result()
            except Exception as exc:
                print('%r generated an exception: %s' % (future, exc))
            else:
                filters.update(data)
                jobs_list[j].update(filters)
            j+=1
    return jobs_list, html_refs

def get_page_and_process(args):
    url, start, filters, languages, job_types = args
    driver = webdriver.Chrome()
    current_url = get_current_url_with_start(url, start)
    html = get_html(current_url, driver)
    content = BeautifulSoup(html, 'lxml')
    jobs_list_new, html_refs_new = scrape_job_details(content, filters, languages, job_types)
    return jobs_list_new

def scrap_pages_by_pages(url, filters, driver, languages, job_types, content=None):
    jobs_list = []
    if content is None:
        html = get_html(url, driver)
        content = BeautifulSoup(html, 'lxml')
    page_num, _ = content.select('.jobsearch-JobCountAndSortPane-jobCount > span')[0].get_text().strip().split(" ")
    jobs_list_new, _ = scrape_job_details(content, filters, languages, job_types)
    jobs_list.extend(jobs_list_new)
    args = ((url, b, languages, job_types) for b in range(50, int(page_num.replace(",","")), 50))
    for b in range(50, int(page_num.replace(",","")), 50):
        data = get_page_and_process((url, b, languages, job_types))
        jobs_list.extend(data)
    # with concurrent.futures.ProcessPoolExecutor() as executor:
    #     futures = executor.map(get_page_and_process, args)
    #     for future in concurrent.futures.as_completed(futures):
    #         try:
    #             data = future.result()
    #         except Exception as exc:
    #             print('%r generated an exception: %s' % (future, exc))
    #         else:
    #             jobs_list.extend(data)
    return jobs_list