import requests
from bs4 import BeautifulSoup

page = requests.get("https://www1.nyc.gov/nyc-resources/agencies.page")

soup = BeautifulSoup(page.text, 'html.parser')
a_list = soup.find_all('a')
agencies = []

# Lines 72 up to the last 24 lines are the ones that have agency names
agencies = [a.get_text() for a in a_list[71:len(a_list)-24]]

agencies_without_acronym = []
acronyms = []
for agency in agencies:
    words = agency.split()
    acronym = agency.split()[-1]
    if acronym[0] is '(' and  acronym[-1] is ')':
        acronyms.append(acronym[1:-1])
        agencies_without_acronym.append(words[:-1])

# use this for printing acronym
for acronym in acronyms:
    print(acronym)

# use this for printing long description of agency
#for agency in agencies_without_acronym:
#    print(' '.join([str(elem) for elem in agency]))
