import bonobo
import json
import re
import requests
import tldextract
import uuid
import yaml

from bonobo.config import use
from bonobo.constants import NOT_MODIFIED
from datetime import datetime
from furl import furl
from peewee import (
    Model,
    CharField,
    TextField,
    AutoField,
    DateTimeField,
    ForeignKeyField,
    CompositeKey,
)
from playhouse.sqlite_ext import SqliteExtDatabase as SqliteDatabase
from slugify import slugify
from ucache import SqliteCache
from urlextract import URLExtract
import time

# Number of sections to keep trying to write while db is locked
DESTDB_LOCKED_RETRIES_COUNT = 10

# we're going to initialize this later
destDB = SqliteDatabase(None)


class BaseModel(Model):
    class Meta:
        database = destDB
        legacy_table_names = False


class Execution(BaseModel):
    id = AutoField()
    created_on = DateTimeField()
    config = TextField()


class Namespace(BaseModel):
    id = CharField(primary_key=True, null=True)
    execution = ForeignKeyField(Execution, backref="execution", column_name="execution_id")
    created_on = DateTimeField()


class SourceText(BaseModel):
    execution = ForeignKeyField(Execution, backref="execution", column_name="execution_id")
    namespace = ForeignKeyField(Namespace, backref="namespace", column_name="namespace_id")
    text_id = CharField(index=True)
    text = TextField()

    class Meta:
        primary_key = CompositeKey("namespace", "text_id")


class Content(BaseModel):
    id = AutoField()
    namespace = ForeignKeyField(Namespace, backref="namespace", column_name="namespace_id")
    execution = ForeignKeyField(Execution, backref="execution", column_name="execution_id")
    source_text_id = CharField()
    created_on = DateTimeField()
    final_url = TextField()
    orig_url = TextField()
    link_brand_fqdn = CharField()
    path_slug = TextField()

    class Meta:
        indexes = ((("namespace_id", "source_text_id", "final_url"), True),)


class TextPattern:
    def __init__(self, pattern_str, ignore_case, replace_with=None):
        self.pattern_str = pattern_str
        flags = re.IGNORECASE if ignore_case == True else 0
        self.reg_exp = re.compile(pattern_str, flags)
        self.replace_with = "" if replace_with is None else replace_with

    def matches(self, text):
        return self.reg_exp.match(text)

    def replace_all(self, orig_text):
        return str(self.reg_exp.sub(self.replace_with, orig_text))


class TextPatterns:
    def __init__(self, patterns):
        self.patterns = patterns

    def match_any(self, text):
        for p in self.patterns:
            if p.matches(text):
                return True
        return False

    def replace_all(self, orig_text):
        new_text = orig_text
        for p in self.patterns:
            new_text = p.replace_all(new_text)
        return new_text


class Configuration:
    def __init__(self, config_url):
        self.config_url = config_url
        try:
            with open(config_url) as configfile_contents:
                config = yaml.safe_load(configfile_contents)
            self.source = config["source"]
            self.destination = config["destination"]
            self.urls_cache_db = config["caches"]["urls"]["db"]
            self.urls_cache_item_expire_secs = 60 * 60 * 24 * 30
            self.http_request_timeout_secs = config["http_request_timeout_secs"]
            self.ignore_url_patterns = TextPatterns(
                [
                    TextPattern(p["reg_exp_pattern_str"], p["ignore_case"])
                    for p in config["ignore_url_patterns"]
                ]
            )
            self.remove_params_from_url_query_strs = TextPatterns(
                [
                    TextPattern(p["reg_exp_pattern_str"], p["ignore_case"])
                    for p in config["remove_params_from_url_query_strs"]
                ]
            )
            self.link_brand_formatters = TextPatterns(
                [
                    TextPattern(
                        p["find_reg_exp_pattern_str"],
                        p["ignore_case"],
                        replace_with=p["replace_reg_exp_pattern_str"],
                    )
                    for p in config["link_brand_formatters"]
                ]
            )
        except Exception as e:
            print("Unable to load config from URL: ", config_url, str(e))
            exit(-1)


class Link:
    def __init__(self, config, url, http_status_code, url_after_redirects=None, message=None):
        self.orig_url = url
        self.http_status_code = http_status_code
        self.url_after_redirects = url_after_redirects
        self.error_message = message

    def is_valid(self):
        return True if self.http_status_code == 200 else False

    def is_ignored(self):
        return True if self.http_status_code == -2 else False

    def cleaned(self, config):
        clean_furl = furl(self.url_after_redirects)
        params_removed = []
        for arg in clean_furl.args:
            if config.remove_params_from_url_query_strs.match_any(arg):
                params_removed.append(arg)
        for param in params_removed:
            del clean_furl.args[param]
        link_brand_fqdn = config.link_brand_formatters.replace_all(
            tldextract.extract(clean_furl.url).fqdn
        )
        return clean_furl.url, link_brand_fqdn, slugify(str(clean_furl.path))


class LinkFactory:
    url_extractor = URLExtract()

    def __init__(self, config):
        self.config = config
        self.url_cache = SqliteCache(config.urls_cache_db)
        self.http = requests.Session()
        self.http.headers = {"User-Agent": "Lectio"}

    def parse(self, url):
        link = self.url_cache.get(url)
        if not link is None:
            return link
        try:
            if self.config.ignore_url_patterns.match_any(url):
                link = Link(self.config, url, -2, message="Ignored")
            else:
                resp = self.http.head(
                    url, allow_redirects=True, timeout=self.config.http_request_timeout_secs,
                )
                if resp.status_code == 200:
                    link = Link(self.config, url, resp.status_code, url_after_redirects=resp.url)
                else:
                    link = Link(
                        self.config, url, -1, message="Invalid HTTP Status Code " + resp.status_code
                    )
        except Exception as e:
            link = Link(self.config, url, -1, message=str(e))
        self.url_cache.set(url, link, config.urls_cache_item_expire_secs)
        return link

    def close(self):
        self.url_cache.close()


@use("config", "source_data_db", "execution")
def consume_source_rows(config, source_data_db, execution):
    for row in source_data_db.execute_sql(config.source["rows_sql"]):
        for _ in range(0, DESTDB_LOCKED_RETRIES_COUNT):
            try:
                source_text, created = SourceText.get_or_create(
                    namespace_id=config.source["namespace"],
                    text_id=str(row[config.source["identify_urls_in_text_sql_col_index"]]),
                    defaults={
                        "text": row[config.source["extract_urls_from_text_sql_col_index"]],
                        "execution": execution,
                    },
                )
            except:
                time.sleep(1)
                pass
            finally:
                break
        else:
            return False
        yield source_text, created


def extract_urls(source_text, created):
    for url in LinkFactory.url_extractor.find_urls(source_text.text, True):
        yield url, source_text


@use("config", "link_factory")
def parse_urls(url, source_text, config, link_factory):
    return url, source_text, link_factory.parse(url)


def filter_ignore_urls(url, source_text, link):
    if link.is_ignored():
        return False
    else:
        return url, source_text, link


def filter_valid_urls(url, source_text, link):
    if link.is_valid():
        return url, source_text, link
    else:
        return False


@use("config", "execution", "namespace")
def save_content(url, source_text, link, config, execution, namespace):
    final_url, link_brand_fqdn, path_slug = link.cleaned(config)
    # in case the database is locked due to concurrent writes,
    # keep trying before skipping the url
    for _ in range(0, DESTDB_LOCKED_RETRIES_COUNT):
        try:
            content, created = Content.get_or_create(
                namespace=namespace,
                source_text_id=source_text.text_id,
                final_url=final_url,
                defaults={
                    "execution": execution,
                    "link_brand_fqdn": link_brand_fqdn,
                    "orig_url": link.orig_url,
                    "path_slug": path_slug,
                    "created_on": datetime.now(),
                },
            )
        except:
            time.sleep(1)
            pass
        finally:
            break
    else:
        return False
    yield content, created


def get_graph(config):
    graph = bonobo.Graph()
    graph.add_chain(
        consume_source_rows,
        extract_urls,
        parse_urls,
        filter_ignore_urls,
        filter_valid_urls,
        save_content,
    )
    return graph


def get_services(config, link_factory):
    execution = Execution.create(
        created_on=datetime.now(),
        config=json.dumps(
            config,
            default=lambda o: o.__dict__ if hasattr(o, "__dict__") else str(o),
            sort_keys=True,
            indent=4,
        ),
    )
    namespace, _ = Namespace.get_or_create(
        id=config.source["namespace"],
        defaults={"created_on": datetime.now(), "execution": execution},
    )
    return {
        "execution": execution,
        "namespace": namespace,
        "config": config,
        "source_data_db": SqliteDatabase(
            config.source["db"],
            pragmas={"journal_mode": "wal", "cache_size": -1024 * 64, "query_only": True},
        ),
        "link_factory": link_factory,
    }


if __name__ == "__main__":
    parser = bonobo.get_argument_parser()
    parser.add_argument("--config-url", action="store", required=True)

    with bonobo.parse_args(parser) as options:
        config = Configuration(**options)
        link_factory = LinkFactory(config)
        destDB.init(
            config.destination["db"],
            pragmas={"journal_mode": "wal", "cache_size": -1024 * 64, "busy_timeout": 5000},
        )
        destDB.connect()
        destDB.create_tables([Execution, Namespace, SourceText, Content])
        bonobo.run(get_graph(config), services=get_services(config, link_factory))
        link_factory.close()
        destDB.close()
