import abc
import functools
import multiprocessing as mp
import time
from typing import Optional, List, Tuple

import bs4
import pandas as pd
import requests
import os
import string
import joblib
from tqdm.auto import tqdm

letters = list(string.ascii_lowercase)


def safe_get_soup(url, logfile=None, **kwargs):
    try:
        with requests.get(url, timeout=5, **kwargs) as resp:
            soup = bs4.BeautifulSoup(resp.content, features='lxml')
            return soup
    except Exception as ex:
        print(f'encountered {ex}, when fetching {url}', file=logfile)
        print('sleeping for five seconds...', file=logfile)
        time.sleep(5)
    return safe_get_soup(url, logfile=logfile)


class ListScraper(abc.ABC):
    @abc.abstractmethod
    def list_attorneys(self) -> pd.DataFrame:
        """Generates the list of attorneys for the given source"""

    def _cache_load_term_frame(self, term) -> Optional[pd.DataFrame]:
        term_frame_path = os.path.join(self._cache_path, f'{term}.pkl')
        if not os.path.exists(term_frame_path):
            return None
        frame = pd.read_pickle(term_frame_path)
        return frame

    def _cache_dump_term_frame(self, term, frame: pd.DataFrame):
        term_frame_path = os.path.join(self._cache_path, f'{term}.pkl')
        return frame.to_pickle(term_frame_path)

    def __init__(self, cache_path, **kwargs):
        self._cache_path = cache_path
        if not os.path.exists(self._cache_path):
            os.mkdir(self._cache_path)


class DetailsScraper(abc.ABC):
    @abc.abstractmethod
    def _list_urls(self, attorneys: pd.DataFrame) -> List[str]:
        """Returns the list of urls from attorneys"""

    @abc.abstractmethod
    def _page_details(self, page_url) -> Tuple[str, dict]:
        """
        Fetches attorney details from the attorney page.
        Returns the details in the (page_url, details_dict) format
        """

    def _cache_load_term_frame(self, term) -> Optional[pd.DataFrame]:
        term_frame_path = os.path.join(self._cache_path, f'{term}.pkl')
        if not os.path.exists(term_frame_path):
            return None
        frame = pd.read_pickle(term_frame_path)
        return frame

    def _cache_dump_term_frame(self, term, frame: pd.DataFrame):
        term_frame_path = os.path.join(self._cache_path, f'{term}.pkl')
        return frame.to_pickle(term_frame_path)

    def fetch_details(self, attorneys: pd.DataFrame) -> pd.DataFrame:
        """Fetch details about each row in `attorneys`"""
        attorneys_hash = joblib.hash(attorneys)
        cached_details = self._cache_load_term_frame(attorneys_hash)
        if cached_details is not None:
            return cached_details

        page_urls = self._list_urls(attorneys)
        page_list, details_list = [], []
        if self._processes is not None:
            with mp.Pool(processes=mp.cpu_count()) as pool:
                for page, page_details in tqdm( 
                        pool.imap_unordered(self._page_details, page_urls), total=len(page_urls)):
                    details_list.append(page_details)
                    page_list.append(page)
        else:
            for page in tqdm(page_urls):
                page_details = self._page_details(page)
                details_list.append(page_details)
                page_list.append(page)

        details = pd.DataFrame(details_list, index=page_list)

        self._cache_dump_term_frame(attorneys_hash, details)
        return details

    def __init__(self, cache_path, processes: Optional[int] = None):
        self._cache_path = cache_path
        self._processes = processes


class AttorneysScraper(abc.ABC):
    @staticmethod
    def combine_details(attorneys: pd.DataFrame,
                        details: pd.DataFrame) -> pd.DataFrame:
        return attorneys.join(details)

    def scrape(self):
        attorneys = self._list_scraper.list_attorneys()
        details = self._details_scraper.fetch_details(attorneys)
        combined = self.combine_details(attorneys, details)
        return combined

    def __init__(self, cache_path: str, name: str,
                 list_scraper: type, details_scraper: type,
                 processes=None):

        self._cache_path = os.path.join(cache_path, name)
        if not os.path.exists(self._cache_path):
            os.mkdir(self._cache_path)

        if processes is None:
            # We allow user to omit the argument and choose processes
            # automatically as per the number of CPUs
            processes = mp.cpu_count()
        elif isinstance(processes, str) and processes == 'none':
            # If user sets processes to none, we disable multiprocessing
            processes = None

        self._list_scraper: ListScraper = (
            list_scraper(cache_path=self._cache_path, processes=processes)
        )
        self._details_scraper: DetailsScraper = (
            details_scraper(cache_path=self._cache_path, processes=processes)
        )


class ListByLettersScraper(ListScraper):
    _processes: int

    @abc.abstractmethod
    def _list_by_letter_internal(self, letter) -> pd.DataFrame:
        pass

    def _list_by_letter(self, letter):
        cached_frame = self._cache_load_term_frame(letter)
        if cached_frame is not None:
            return cached_frame
        letter_terms = self._list_by_letter_internal(letter)
        frame = pd.DataFrame(letter_terms)
        self._cache_dump_term_frame(letter, frame)
        return frame

    def list_attorneys(self):
        if self._processes is not None:
            with mp.Pool(processes=self._processes) as pool:
                func = functools.partial(self._list_by_letter)
                mapped = pool.map(func, letters)
        else:
            mapped = []
            for letter in letters:
                letter_frame = self._list_by_letter(letter)
                mapped.append(letter_frame)
        frame = pd.concat(mapped).drop_duplicates(subset=['href'])
        return frame

    def __init__(self, processes=None, **kwargs):
        super(ListByLettersScraper, self).__init__(**kwargs)
        self._processes = processes
