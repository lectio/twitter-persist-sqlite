import bonobo
import sqlalchemy as db
import re

from bonobo.config import use
from bonobo.constants import NOT_MODIFIED
from furl import furl
from urlextract import URLExtract

@use('engine', 'url_extractor')
def get_tweets_with_urls(engine, url_extractor):
    connection = engine.connect()
    result = connection.execute("select id, full_text from tweets limit 50")
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

@use('http', 'http_request_timeout_secs')
def filter_valid_urls(url, tweet, http, http_request_timeout_secs):
    try:
        resp = http.head(url, allow_redirects=True, timeout=http_request_timeout_secs)
        if resp.status_code == 200:
            yield resp.url, tweet 
        else:
            return False
    except:
        return False

@use('remove_params_from_url_query_strs')
def clean_url_params(url, tweet, remove_params_from_url_query_strs):
    newUrl = furl(url)
    remove_params = []
    for arg in newUrl.args:
        for p in remove_params_from_url_query_strs:
            if p.match(arg):
                remove_params.append(arg)
    for param in remove_params:
        del newUrl.args[param]
    yield newUrl, remove_params, tweet

def get_graph(**options):
    graph = bonobo.Graph()
    graph.add_chain(
        get_tweets_with_urls,
        filter_ignored_urls,
        filter_valid_urls,
        clean_url_params,
        #bonobo.PrettyPrinter(),
    )
    return graph

def get_services(sql_engine_url, use_http_cache=False, http_request_timeout_secs=5):
    engine = db.create_engine(sql_engine_url)
    url_extractor = URLExtract()
    ignore_url_patterns = [
        re.compile('^https://twitter.com/(.*?)/status/(.*)$', re.IGNORECASE),
        re.compile('https://t.co')
    ]
    remove_params_from_url_query_strs = [
        re.compile('^utm_')
    ]
    if use_http_cache:
        from requests_cache import CachedSession
        http = CachedSession('http.cache')
    else:
        import requests
        http = requests.Session()
    http.headers = {'User-Agent': 'Monkeys!'}
    return { 
        'engine': engine,
        'url_extractor' : url_extractor,
        'ignore_url_patterns' : ignore_url_patterns,
        'http' : http,
        'http_request_timeout_secs' : http_request_timeout_secs,
        'remove_params_from_url_query_strs' : remove_params_from_url_query_strs
    }

if __name__ == '__main__':
    parser = bonobo.get_argument_parser()
    parser.add_argument('--use-http-cache', action='store_true', default=False)
    parser.add_argument('--http-request-timeout-secs', action='store', default=5)
    parser.add_argument('--sql-engine-url', action='store', required=True)

    with bonobo.parse_args(parser) as options:
        bonobo.run(get_graph(**options), services=get_services(**options))
