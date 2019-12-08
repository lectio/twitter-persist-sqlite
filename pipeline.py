import bonobo
import json
import pickle
import re
import requests
import sqlalchemy as db
import sqlite3
import tldextract
import uuid

from bonobo.config import use
from bonobo.constants import NOT_MODIFIED
from furl import furl
from slugify import slugify
from urlextract import URLExtract

urlCacheDB_DDL_init = [
    "CREATE TABLE IF NOT EXISTS url_cache(url TEXT PRIMARY KEY, http_status_code INT, tweet_id INT, tweet BLOB, url_after_redirects TEXT, message TEXT)",
    "CREATE INDEX IF NOT EXISTS url_cache_pk ON url_cache(url)",
    "CREATE INDEX IF NOT EXISTS url_cache_tweet_id ON url_cache(tweet_id)",
    "CREATE TABLE IF NOT EXISTS url_content(final_url TEXT PRIMARY KEY, orig_url TEXT, link_brand_fqdn TEXT, slug TEXT, tweet JSON, tweet_id INT)",
    "CREATE INDEX IF NOT EXISTS url_content_pk ON url_content(final_url)",
    "CREATE INDEX IF NOT EXISTS url_content_tweet_id ON url_content(tweet_id)"
]

@use('content_unprocessed_engine', 'url_extractor')
def get_tweets_with_urls(content_unprocessed_engine, url_extractor):
    connection = content_unprocessed_engine.connect()
    result = connection.execute("select id, full_text from tweets")
    for tweet in result:
        for url in url_extractor.find_urls(tweet['full_text'], True):
            yield url, tweet
    connection.close()

@use('ignore_url_patterns')
def filter_ignored_urls(url, tweet, ignore_url_patterns):
    for p in ignore_url_patterns:
        if p.match(url):
            return False
    yield NOT_MODIFIED 

@use('content_processed_db_conn', 'http', 'http_request_timeout_secs')
def filter_valid_urls(url, tweet, content_processed_db_conn, http, http_request_timeout_secs):
    with content_processed_db_conn as conn:
        for row in conn.execute('SELECT http_status_code, tweet, url_after_redirects FROM url_cache WHERE url = ?', (url,)):
            if row[0] == 200:
                cached_tweet = pickle.loads(row[1])
                return row[2], cached_tweet, url
            else:
                return False

        try:
            resp = http.head(url, allow_redirects=True, timeout=http_request_timeout_secs)
            conn.execute('INSERT INTO url_cache (http_status_code, url, tweet_id, tweet, url_after_redirects) VALUES (?, ?, ?, ?, ?)', (resp.status_code, url, tweet['id'], pickle.dumps(tweet), resp.url))
            if resp.status_code == 200:
                return resp.url, tweet, url
            else:
                return False            
        except Exception as e:
            conn.execute('INSERT INTO url_cache (http_status_code, message, url, tweet_id, tweet) VALUES (?, ?, ?, ?, ?)', (-1, str(e), url, tweet['id'], pickle.dumps(tweet)))
            return False

@use('remove_params_from_url_query_strs')
def clean_url_params(url_after_redirects, tweet, orig_url, remove_params_from_url_query_strs):
    final_furl = furl(url_after_redirects)
    params_removed = []
    for arg in final_furl.args:
        for p in remove_params_from_url_query_strs:
            if p.match(arg):
                params_removed.append(arg)
    for param in params_removed:
        del final_furl.args[param]
    yield final_furl, tweet, orig_url, params_removed

@use('content_processed_db_conn')
def process_url_content(final_furl, tweet, orig_url, params_removed, content_processed_db_conn):
    final_url = final_furl.url
    with content_processed_db_conn as conn:
        for row in conn.execute('SELECT final_url FROM url_content WHERE final_url = ?', (final_url,)):
            return False
        link_brand = tldextract.extract(final_url)
        link_brand_fqdn = re.sub(r"^www[0-9]?\.", "", link_brand.fqdn)
        slug = slugify(str(final_furl.path))
        conn.execute('INSERT INTO url_content (final_url, orig_url, link_brand_fqdn, slug, tweet_id, tweet) VALUES (?, ?, ?, ?, ?, ?)', (str(final_url), str(orig_url), link_brand_fqdn, str(slug), tweet['id'], json.dumps(dict(tweet))))
        return final_url, tweet, orig_url, params_removed, link_brand, slug 

def get_graph(**options):
    graph = bonobo.Graph()
    graph.add_chain(
        get_tweets_with_urls,
        filter_ignored_urls,
        filter_valid_urls,
        clean_url_params,
        process_url_content,
    )
    return graph

def get_services(content_unprocessed_db_url, content_processed_db, http_request_timeout_secs):
    content_unprocessed_engine = db.create_engine(content_unprocessed_db_url)
    url_extractor = URLExtract()
    ignore_url_patterns = [
        re.compile('^https://twitter.com/(.*?)/status/(.*)$', re.IGNORECASE),
        re.compile('https://t.co')
    ]
    remove_params_from_url_query_strs = [
        re.compile('^utm_')
    ]

    http = requests.Session()
    http.headers = {'User-Agent': 'Lectio'}

    content_processed_db_conn = sqlite3.Connection(content_processed_db, timeout=60, check_same_thread=False)
    with content_processed_db_conn as conn:
        for ddl in urlCacheDB_DDL_init:
            try:
                conn.execute(ddl)
            except Exception as e:
                print("Unable to execute DDL: ", ddl, str(e))
                exit(-1)

    return { 
        'content_unprocessed_engine': content_unprocessed_engine,
        'url_extractor' : url_extractor,
        'ignore_url_patterns' : ignore_url_patterns,
        'http' : http,
        'content_processed_db_conn' : content_processed_db_conn,
        'http_request_timeout_secs' : http_request_timeout_secs,
        'remove_params_from_url_query_strs' : remove_params_from_url_query_strs
    }

if __name__ == '__main__':
    parser = bonobo.get_argument_parser()
    parser.add_argument('--content-unprocessed-db-url', action='store', required=True)
    parser.add_argument('--content-processed-db', action='store', required=True)
    parser.add_argument('--http-request-timeout-secs', action='store', required=True, type=int)

    with bonobo.parse_args(parser) as options:
        bonobo.run(get_graph(**options), services=get_services(**options))
