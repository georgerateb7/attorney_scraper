import argparse
import pandas as pd
from typing import List, Tuple
import re

from scrapers.attorneys.base import (
    safe_get_soup, AttorneysScraper, ListByLettersScraper, DetailsScraper)

search_url = 'https://www.osbar.org/members/membersearch.asp'
member_url = 'https://www.osbar.org/members/membersearch_display.asp'


def _get_page_count(soup):
    texts = list(map(lambda x: x.text,
                     soup.find(class_='pagingheader').findAll('h3')))
    for text in texts:
        match = re.match(r'^Page\s(?P<cur>\d+)\sof\s(?P<tot>\d+)$', text)
        if match is None:
            continue
        total = match.groupdict()['tot']
        return int(total)
    return None


def _page_rows(soup) -> list:
    trs = (soup.find('table', id='tblResults')
           .find('tbody').findAll('tr'))
    rows = []
    for tr in trs:
        rows.append(tuple(td.text for td in tr.findAll('td')))
    return rows


def _fetch_list_rows(letter, cp):
    soup = None
    try:
        params = {'last': letter, 'cp': cp}
        soup = safe_get_soup(search_url, params=params)
        return _page_rows(soup)
    except Exception as ex:
        print(f'error fetching {letter}, {cp}')
        if soup is None:
            raise ex
        with open(f'/tmp/{letter}-{cp}.html') as html:
            html.write(soup.prettify())
        raise ex


def _compare_row_letter(row, letter) -> int:
    lhs, rhs = row[1][0].lower(), letter[0].lower()
    comp = ord(lhs) - ord(rhs)
    if comp == 0:
        return comp
    return comp // abs(comp)


def _find_letter_start_page(letter, start, end) -> int:
    """Binary search through pages to identify the start page"""
    if start == end:
        return start

    mid = (start + end) // 2
    rows = _fetch_list_rows(letter, mid)

    first_comp = _compare_row_letter(rows[0], letter)
    last_comp = _compare_row_letter(rows[-1], letter)

    if first_comp == last_comp >= 0:
        return _find_letter_start_page(letter, start, mid)

    if first_comp == last_comp < 0:
        return _find_letter_start_page(letter, mid + 1, end)

    if first_comp < last_comp == 0:
        return mid

    if first_comp < last_comp > 0:
        return _find_letter_start_page(letter, start, mid)

    raise Exception(f'check binary search {letter}, {start}, {end}')


def _fetch_letter_list(letter, start_page, n_total) -> list:
    rows = _fetch_list_rows(letter, start_page)
    letter_rows = []
    for row in rows:
        if _compare_row_letter(row, letter) == 0:
            letter_rows.append(row)

    if start_page == n_total:
        return letter_rows

    if _compare_row_letter(rows[-1], letter) > 0:
        return letter_rows

    next_rows = _fetch_letter_list(letter, start_page + 1, n_total)
    return letter_rows + next_rows


def _oregon_list_by_letter(letter):
    params = {'last': letter, 'cp': 1}
    soup = safe_get_soup(search_url, params=params)
    n_pages = _get_page_count(soup)
    start_page = _find_letter_start_page(letter, 1, n_pages)
    letter_list = _fetch_letter_list(letter, start_page, n_pages)
    return letter_list


class OregonListByLetters(ListByLettersScraper):
    def _list_by_letter_internal(self, letter) -> pd.DataFrame:
        letter_list = _oregon_list_by_letter(letter)
        frame = pd.DataFrame(letter_list,
                             columns=['bar_num', 'name', 'city'])
        frame['href'] = member_url + '?b=' + frame['bar_num']
        return frame


def attorney_details(page_url):
    fields = {'mstatus': 'status', 'madmitdate': 'admit_date',
              'mphone': 'phone', 'memail': 'email'}
    soup = safe_get_soup(page_url)
    table = soup.find('table', id='tbl_member')
    trs = table.findAll('tr')

    details = {}
    for tr in trs:
        tds = tr.findAll('td')
        if len(tds) < 2:
            continue
        id_ = tds[1].attrs.get('id')
        if id_ is not None and id_ not in fields.keys():
            if id_ != 'mnum':
                print(f'passing {id_}')
            continue
        elif id_ is not None and id_ in fields:
            details[fields.get(tds[1].attrs['id'])] = tds[1].text.strip()
            if id_ == 'memail':
                email_td = table.find('td', id='memail')
                if email_td is None:
                    continue
                email_a = email_td.find('a')
                if email_a is None:
                    continue
                href = email_a.attrs['href']
                text = email_a.text

                failed_message = f'check page url {page_url}'
                try:
                    assert href == 'mailto:' + text, failed_message
                except AssertionError as aex:
                    print('AssertionError:', aex)
        else:
            details[tds[0].text.strip()] = tds[1].text.strip()
    return page_url, details


class OregonAttorneyDetails(DetailsScraper):
    def _list_urls(self, attorneys: pd.DataFrame) -> List[str]:
        return attorneys['href'].tolist()

    def _page_details(self, page_url) -> Tuple[str, dict]:
        details = attorney_details(page_url)
        return details


def main(output, cache_path):
    scraper = AttorneysScraper(cache_path=cache_path,
                               name='oregon',
                               list_scraper=OregonListByLetters,
                               details_scraper=OregonAttorneyDetails,
                               )
    attorneys = scraper.scrape()
    attorneys.to_csv(output)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('output')
    parser.add_argument('cache')
    args = parser.parse_args()
    main(args.output, args.cache)
