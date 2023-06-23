import argparse

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.firefox.options import Options

import pickle
import os
import itertools
import string
import time
import functools
import traceback
import multiprocessing as mp
from tqdm.auto import tqdm


class NYScraperError(Exception):
    pass


def enter_search(driver, first, last):
    first_name = WebDriverWait(driver, 30).until(
        expected_conditions.presence_of_element_located((
            By.NAME,
            'wmcSearchTabs:pnlAttorneySearch:nameSearchPanel:strFirstName'
        ))
    )
    first_name.send_keys(first)

    last_name = driver.find_element(
        By.NAME,
        'wmcSearchTabs:pnlAttorneySearch:nameSearchPanel:strLastName'
    )
    last_name.send_keys(last)

    search_btn = driver.find_element(By.NAME, 'btnSubmit')
    search_btn.send_keys(Keys.ENTER)


def second_window_opened(driver):
    if len(driver.window_handles) == 1:
        raise NYScraperError
    return True


def scrape_one_page(driver, skip_wait=False):
    if not skip_wait:
        try:
            error_box = WebDriverWait(driver, 2).until(
                expected_conditions.presence_of_element_located((
                    By.CLASS_NAME, 'CONT_MsgBox_Error'
                ))
            )
            if error_box is not None:
                return []
            raise NYScraperError('unexpected error')
        except TimeoutException as te:
            try:
                tbody = driver.find_element(By.TAG_NAME, 'tbody')
            except NoSuchElementException:
                raise te
    else:
        tbody = WebDriverWait(driver, 10).until(
            expected_conditions.presence_of_element_located((
                By.TAG_NAME, 'tbody'
            ))
        )

    # presence_of_all_elements_located
    trs = tbody.find_elements(By.TAG_NAME, 'tr')

    attorneys = []
    for tr in trs:
        try:
            a = tr.find_element(By.TAG_NAME, 'a')
        except NoSuchElementException:
            continue
        a.click()

        (WebDriverWait(driver, 30, ignored_exceptions=[NYScraperError])
         .until(second_window_opened))

        driver.switch_to.window(driver.window_handles[1])

        cont = WebDriverWait(driver, 30).until(
            expected_conditions.presence_of_element_located((
                By.CLASS_NAME, 'CONT_Default'
            ))
        )
        rows = cont.find_elements(By.CLASS_NAME, 'CONT_Row')
        details = {}
        for row in rows:
            spans = row.find_elements(By.CLASS_NAME, 'CONT_Cell')
            if len(spans) != 2:
                continue
            details[spans[0].text.strip()] = spans[1].text.strip()
        attorneys.append(details)

        driver.close()
        driver.switch_to.window(driver.window_handles[0])

    return attorneys


def search_one_term(first, last, cache):
    fname = f'{first}-{last}.pkl'
    cache_path = os.path.join(cache, fname)

    if os.path.exists(cache_path):
        with open(cache_path, 'rb') as pkl:
            return pickle.load(pkl)

    options = Options()
    options.headless = True
    driver = webdriver.Firefox(options=options)

    try:
        driver.get('https://iapps.courts.state.ny.us/attorneyservices/search')
        enter_search(driver, first, last)

        term_attorneys, next_disabled, skip_wait = [], 'false', False
        while next_disabled == 'false':
            page_attorneys = scrape_one_page(driver, skip_wait=skip_wait)
            term_attorneys.extend(page_attorneys)
            try:
                next_btn = driver.find_element(By.CLASS_NAME, 'next')
            except NoSuchElementException:
                next_disabled = 'true'
                continue
            next_disabled = next_btn.get_attribute('disabled') or 'false'
            if next_disabled == 'false':
                next_btn.click()
            skip_wait = True

        print(f'writing {fname}, size {len(term_attorneys)}')
        with open(cache_path, 'wb') as pkl:
            pickle.dump(term_attorneys, pkl)
        return term_attorneys
    finally:
        driver.quit()


def robust_wrapper(args, func):
    results, i = None, 3
    while results is None and i > 0:
        try:
            results = func(*args)
        except Exception as ex:
            i -= 1
            traceback.print_exc()
            print(f'failed to run {args} for {i}th time; {ex}')
            time.sleep(5)
    return results


def make_iterator(cache):
    looper = itertools.product(string.ascii_lowercase,
                               string.ascii_lowercase,
                               string.ascii_lowercase)
    for x1, x2, x3 in looper:
        first, last = x1, x2 + x3
        args = (first, last, cache)
        yield args


def main(cache, output, multiproc=False):
    if not os.path.exists(cache):
        os.mkdir(cache)

    func = functools.partial(robust_wrapper, func=search_one_term)
    looper, results = make_iterator(cache), []

    if multiproc:
        pool = mp.Pool(processes=int(4 * mp.cpu_count()))
        for result in tqdm(pool.imap_unordered(func, looper), total=26 ** 3):
            if isinstance(result, list):
                results.extend(result)
    else:
        for args_ in tqdm(looper, total=26 ** 3):
            result = func(args_)
            if isinstance(result, list):
                results.extend(result)

    import pandas as pd
    pd.DataFrame(results).to_csv(output)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('cache')
    parser.add_argument('output')
    parser.add_argument('--multiproc', action='store_true')
    args = parser.parse_args()
    main(args.cache, args.output, args.multiproc)
