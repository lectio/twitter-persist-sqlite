import bonobo
import json
import pickle
import re
import requests
import sqlalchemy as db
import sqlite3
import tldextract
import uuid
import yaml

from bonobo.config import use
from bonobo.constants import NOT_MODIFIED
from furl import furl
from slugify import slugify
from urlextract import URLExtract

processedContentDB_SQL_DDL = [
    "CREATE TABLE IF NOT EXISTS url_source(id INTEGER PRIMARY KEY AUTOINCREMENT, engine_url TEXT, created_on TIMESTAMP, remarks TEXT)",
    "CREATE INDEX IF NOT EXISTS url_source_pk ON url_source(id)",

    "CREATE TABLE IF NOT EXISTS url_cache(url TEXT PRIMARY KEY, source_id INTEGER, http_status_code INT, source_row BLOB, url_after_redirects TEXT, message TEXT)",
    "CREATE INDEX IF NOT EXISTS url_cache_pk ON url_cache(url)",

    "CREATE TABLE IF NOT EXISTS url_content(final_url TEXT PRIMARY KEY, orig_url TEXT, link_brand_fqdn TEXT, slug TEXT, source_id INTEGER, source_row JSON)",
    "CREATE INDEX IF NOT EXISTS url_content_pk ON url_content(final_url)",
]

@use('content_unprocessed_engine', 'url_extractor', 'content_unprocessed_db_source_row_sql', 'content_unprocessed_db_source_text_col_name')
def get_text_with_urls(content_unprocessed_engine, url_extractor, content_unprocessed_db_source_row_sql, content_unprocessed_db_source_text_col_name):
    connection = content_unprocessed_engine.connect()
    result = connection.execute(content_unprocessed_db_source_row_sql)
    for row in result:
        for url in url_extractor.find_urls(row[content_unprocessed_db_source_text_col_name], True):
            yield url, row
    connection.close()

@use('config')
def filter_ignored_urls(url, row, config):
    for p in config['ignore_url_patterns']:
        if p['reg_exp'].match(url):
            return False
    yield NOT_MODIFIED 

@use('content_processed_db_conn', 'source_id', 'http', 'http_request_timeout_secs')
def filter_valid_urls(url, row, content_processed_db_conn, source_id, http, http_request_timeout_secs):
    with content_processed_db_conn as conn:
        for row in conn.execute('SELECT http_status_code, source_row, url_after_redirects FROM url_cache WHERE url = ?', (url,)):
            if row[0] == 200:
                cached_source = pickle.loads(row[1])
                return row[2], cached_source, url
            else:
                return False

        try:
            resp = http.head(url, allow_redirects=True, timeout=http_request_timeout_secs)
            conn.execute('INSERT INTO url_cache (source_id, http_status_code, url, source_row, url_after_redirects) VALUES (?, ?, ?, ?, ?)', (source_id, resp.status_code, url, pickle.dumps(row), resp.url))
            if resp.status_code == 200:
                return resp.url, row, url
            else:
                return False            
        except Exception as e:
            conn.execute('INSERT INTO url_cache (source_id, http_status_code, message, url, source_row) VALUES (?, ?, ?, ?, ?)', (source_id, -1, str(e), url, pickle.dumps(row)))
            return False

@use('config')
def clean_url_params(url_after_redirects, row, orig_url, config):
    final_furl = furl(url_after_redirects)
    params_removed = []
    for arg in final_furl.args:
        for p in config['remove_params_from_url_query_strs']:
            if p['reg_exp'].match(arg):
                params_removed.append(arg)
    for param in params_removed:
        del final_furl.args[param]
    yield final_furl, row, orig_url, params_removed

@use('config', 'content_processed_db_conn', 'source_id')
def process_url_content(final_furl, row, orig_url, params_removed, config, content_processed_db_conn, source_id):
    final_url = final_furl.url
    with content_processed_db_conn as conn:
        for _ in conn.execute('SELECT final_url FROM url_content WHERE final_url = ?', (final_url,)):
            return NOT_MODIFIED
        link_brand = tldextract.extract(final_url)
        link_brand_fqdn = link_brand.fqdn
        for f in config['link_brand_formatters']:
            link_brand_fqdn = f['find_reg_exp'].sub(f['replace_reg_exp_pattern_str'], link_brand_fqdn)
        slug = slugify(str(final_furl.path))
        conn.execute('INSERT INTO url_content (source_id, final_url, orig_url, link_brand_fqdn, slug, source_row) VALUES (?, ?, ?, ?, ?, ?)', (source_id, str(final_url), str(orig_url), link_brand_fqdn, str(slug), json.dumps(dict(row))))
        return final_url, row, orig_url, params_removed, link_brand, slug 

def get_graph(**options):
    graph = bonobo.Graph()
    graph.add_chain(
        get_text_with_urls,
        filter_ignored_urls,
        filter_valid_urls,
        clean_url_params,
        process_url_content,
    )
    return graph

def configure(config_url):
    config = {}
    try:
        with open(config_url) as configfile_contents:
            config = yaml.safe_load(configfile_contents)        
        for p in config['ignore_url_patterns'] + config['remove_params_from_url_query_strs']:
            flags = re.IGNORECASE if p['ignore_case'] == True else 0
            p['reg_exp'] = re.compile(p['reg_exp_pattern_str'], flags)
        for f in config['link_brand_formatters']:
            flags = re.IGNORECASE if p['ignore_case'] == True else 0
            f['find_reg_exp'] = re.compile(f['find_reg_exp_pattern_str'], flags)
            if f['replace_reg_exp_pattern_str'] == None:
                f['replace_reg_exp_pattern_str'] = ""
    except Exception as e:
        print("Unable to load config from URL: ", config_url, str(e))
        exit(-1)
    return config

def get_services(config_url, 
        content_unprocessed_db_url, 
        content_unprocessed_db_source_row_sql, content_unprocessed_db_source_text_col_name, 
        content_processed_db, http_request_timeout_secs):
    content_unprocessed_engine = db.create_engine(content_unprocessed_db_url)
    url_extractor = URLExtract()
    config = configure(config_url)

    http = requests.Session()
    http.headers = {'User-Agent': 'Lectio'}

    content_processed_db_conn = sqlite3.Connection(content_processed_db, timeout=60, check_same_thread=False)
    with content_processed_db_conn as conn:
        for ddl in processedContentDB_SQL_DDL:
            try:
                conn.execute(ddl)
            except Exception as e:
                print("Unable to execute DDL: ", ddl, str(e))
                exit(-1)
        result = conn.execute('INSERT INTO url_source (engine_url, created_on) VALUES (?, strftime("%s", CURRENT_TIME))', (str(content_unprocessed_db_url),))
        source_id = result.lastrowid

    return { 
        'content_unprocessed_engine': content_unprocessed_engine,
        'content_unprocessed_db_source_row_sql': content_unprocessed_db_source_row_sql,
        'content_unprocessed_db_source_text_col_name': content_unprocessed_db_source_text_col_name,
        'source_id' : source_id,
        'url_extractor' : url_extractor,
        'http' : http,
        'content_processed_db_conn' : content_processed_db_conn,
        'http_request_timeout_secs' : http_request_timeout_secs,
        'config' : config
    }

if __name__ == '__main__':
    parser = bonobo.get_argument_parser()
    parser.add_argument('--content-unprocessed-db-url', action='store', required=True)
    parser.add_argument('--content-unprocessed-db-source-row-sql', action='store', required=True)
    parser.add_argument('--content-unprocessed-db-source-text-col-name', action='store', required=True)
    parser.add_argument('--content-processed-db', action='store', required=True)
    parser.add_argument('--http-request-timeout-secs', action='store', required=True, type=int)
    parser.add_argument('--config-url', action='store', required=True)

    with bonobo.parse_args(parser) as options:
        bonobo.run(get_graph(**options), services=get_services(**options))
