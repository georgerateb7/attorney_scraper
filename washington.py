#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Aug 27 17:17:39 2022

@author: georgerateb
"""

import pandas as pd
import argparse
from typing import List, Tuple
import numpy as np

from base import (
    safe_get_soup, AttorneysScraper, ListByLettersScraper, DetailsScraper)

search_tpl = 'https://www.mywsba.org/personifyebusiness/LegalDirectory.aspx?ShowSearchResults=TRUE&FirstName={letter}&Page={page}'
member_url = 'https://www.mywsba.org/personifyebusiness/LegalDirectory/LegalProfile.aspx?Usr_ID='

def _get_page_count(soup): 
    pages = soup.find('span',id='dnn_ctr2972_DNNWebControlContainer_ctl00_lblRowCount') #actively removing ALL safeties here
    total = [int(s) for s in pages.text.split() if s.isdigit()][0]
    page_count = total//20
    return page_count


def parse_table(table):
    page = []
    header=['bar_num', 'first_name','last_name', 'city','status','phone']
    trs = table.findAll('tr',{'class':'grid-row'})
    for row in trs:
        row_dict = {}
        for i, cell in enumerate(row.findAll('td')):
            row_dict[header[i]] = cell.text.strip()
        page.append(row_dict)
    return page


def _fetch_letter_list(letter, n_total) -> list:
    results = []
    for i in np.arange(0,n_total+1,1):
        soup = safe_get_soup(search_tpl.format(letter=letter,page=i))
        table = soup.find('table', id='dnn_ctr2972_DNNWebControlContainer_ctl00_dg')
        page = parse_table(table)
        results.extend(page)
    return results

    
def _washington_list_by_letter(letter):
    print(letter + " in washington list by letter function")
    soup = safe_get_soup(search_tpl.format(letter=letter,page=1))
    n_pages = _get_page_count(soup)
    letter_list = _fetch_letter_list(letter, n_pages)
    return letter_list


class WashingtonListByLetters(ListByLettersScraper): 
    def _list_by_letter_internal(self, letter) -> pd.DataFrame:
        letter_list = _washington_list_by_letter(letter)
        frame = pd.DataFrame(letter_list,
                             columns=['bar_num', 'first_name','last_name', 'city','status','phone']) #refer back here
        frame['href'] = member_url + frame['bar_num'].str.zfill(12)
        return frame

# not even sure it's grabbing anything

def attorney_details(page_url): #given a URL
    soup = safe_get_soup(page_url) #get the soup page url
    
    member_details = soup.find(id='dnn_ctr2977_DNNWebControlContainer_ctl00_ContainerPanel')
    
    status = member_details.find('span',id='dnn_ctr2977_DNNWebControlContainer_ctl00_lblStatus')
    admit_date = member_details.find('span',id='dnn_ctr2977_DNNWebControlContainer_ctl00_lblWaAdmitDate')
    phone = member_details.find('span',id='dnn_ctr2977_DNNWebControlContainer_ctl00_lblPhone')
    email = member_details.find('span',id='dnn_ctr2977_DNNWebControlContainer_ctl00_lblEmail')
    
    details = {}      
    
    details['status'] = status.text.strip()
    details['admit_date'] = admit_date.text.strip()
    details['phone'] = phone.text.strip()
    details['email'] = email.text.strip()
    
    return page_url, details

class WashingtonAttorneysScraper(AttorneysScraper):
    @staticmethod
    def combine_details(attorneys: pd.DataFrame,
                        details: pd.DataFrame) -> pd.DataFrame:
        return attorneys.join(details['admit_date','email'])


class WashingtonAttorneyDetails(DetailsScraper): 
    def _list_urls(self, attorneys: pd.DataFrame) -> List[str]: 
        return attorneys['href'].tolist()

    def _page_details(self, page_url) -> Tuple[str, dict]: 
        details = attorney_details(page_url)
        return details


def main(cache, output):
    scraper = WashingtonAttorneysScraper(cache_path=cache, 
                               name='washington', 
                               list_scraper=WashingtonListByLetters,
                               details_scraper=WashingtonAttorneyDetails, 
                               )
    print("scraper created")
    attorneys = scraper.scrape()
    attorneys.to_csv(output)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('cache')
    parser.add_argument('output')
    args = parser.parse_args()
    print("getting to main")
    main(args.cache, args.output)
