import pandas as pd
import argparse
import random
import time
import re
import string
from typing import List, Tuple

from scrapers.attorneys.base import (
    safe_get_soup, AttorneysScraper, ListByLettersScraper, DetailsScraper)

base_url = 'https://apps.calbar.ca.gov'
search_tpl = base_url + '/attorney/LicenseeSearch/QuickSearch?FreeText={term}'
overflow_text = 'Only the first 500 results will be shown.'
letters = list(string.ascii_lowercase)


def parse_table(table):
    if table is None:
        return []

    thead = table.find('thead')
    header = [th.text.strip() for th in thead.findAll('th')]

    tbody = table.find('tbody')
    results = []
    for row in tbody.findAll('tr'):
        row_dict = {}
        for i, cell in enumerate(row.findAll('td')):
            if (a := cell.find('a')) is not None:
                row_dict['href'] = a.attrs['href']
            row_dict[header[i]] = cell.text.strip()
        results.append(row_dict)
    return results


def search_term(term, template, verbose=True, logfile=None, ignore_overflow=False):
    if verbose:
        print(f'searching term "{term}"...', file=logfile)

    soup = safe_get_soup(template.format(term=term))
    table = soup.find('table', id='tblAttorney')
    strong_texts = [strong.text for strong in soup.findAll('strong')]
    if overflow_text in strong_texts and not ignore_overflow:
        results = []
        for letter in letters:
            result = search_term(term + letter, template, verbose=verbose)
            results.extend(result)
        if ' ' not in term:
            for letter in letters:
                result = search_term(
                    term + ' ' + letter, template, verbose=verbose)
                results.extend(result)
    else:
        # derive results from table
        results = parse_table(table)

    if verbose:
        print(f'found {len(results)} results for "{term}"', file=logfile)

    if random.random() > 0.9:
        if verbose:
            print('random timeout!', file=logfile)
        time.sleep(1)
    return results


def find_correct_email(attorney_page):
    hidden = [style for style in attorney_page.findAll('style')
              if style.text.strip().startswith('#e0')]
    if len(hidden) == 0:
        return None
    correct = [item for item in hidden[0].text.strip().split('#')
               if 'inline' in item]
    matched = re.match(r'(?P<cid>e\d+)\{.*', correct[0])
    if matched is not None:
        cid = matched.groupdict()['cid']
        email = attorney_page.find('span', id=cid)
        if email is not None:
            return email.text
        return None
    else:
        return None


def attorney_details(page_url):
    attorney_page = safe_get_soup(base_url + page_url)
    member_details = attorney_page.find(id='moduleMemberDetail')
    ps = [p.text.strip() for p in member_details.findAll('p')]

    details = {}
    for p in ps:
        if p.startswith('Address:'):
            details['address'] = p[len('Address:'):]
        if p.startswith('Phone:'):
            # to be enhanced to remove FAX information
            details['phone'] = p[len('Phone:'):].split('|')[0].strip()

    email = find_correct_email(attorney_page)
    if email is not None:
        details['email'] = email

    website_anchor = member_details.find('a', id='websiteLink')
    if website_anchor is not None:
        details['website'] = website_anchor.text.strip()
    return page_url, details


class CaliforniaListByLetterScraper(ListByLettersScraper):
    def _list_by_letter_internal(self, letter) -> pd.DataFrame:
        letter_terms = search_term(letter, template=search_tpl, verbose=True, ignore_overflow=True)
        return pd.DataFrame(letter_terms)


class CaliforniaDetailsScraper(DetailsScraper):

    def _list_urls(self, attorneys: pd.DataFrame) -> List[str]:
        frame = attorneys.query('Status not in ("Judge", "Deceased")')
        pages = [href for href in frame['href'].tolist()]
        return pages

    def _page_details(self, page_url) -> Tuple[str, dict]:
        return attorney_details(page_url)


def main(output):
    scraper = AttorneysScraper(cache_path='/tmp/cache',
                               name='california',
                               list_scraper=CaliforniaListByLetterScraper,
                               details_scraper=CaliforniaDetailsScraper)
    frame = scraper.scrape()
    frame.to_csv(output)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('output')
    args = parser.parse_args()
    main(args.output)
