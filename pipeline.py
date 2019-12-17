import bonobo
import cgi
import json
import re
import requests
import tldextract
import time
import uuid
import yaml

from bonobo.config import use
from bonobo.constants import NOT_MODIFIED
from datetime import datetime
from dateutil.parser import parse
from furl import furl
from peewee import (
    Model,
    CharField,
    TextField,
    AutoField,
    DateTimeField,
    ForeignKeyField,
    CompositeKey,
    IntegerField,
)
from playhouse.sqlite_ext import SqliteExtDatabase as SqliteDatabase
from slugify import slugify
from urlextract import URLExtract

# Number of sections to keep trying to write while db is locked
DESTDB_LOCKED_RETRIES_COUNT = 10

# we're going to initialize these later
content_db = SqliteDatabase(None)
request_cache_db = SqliteDatabase(None)


class BaseModel(Model):
    class Meta:
        database = content_db
        legacy_table_names = False


class Execution(BaseModel):
    id = AutoField()
    created_on = DateTimeField()
    config = TextField()


class Namespace(BaseModel):
    id = CharField(primary_key=True)
    execution = ForeignKeyField(Execution, column_name="execution_id")
    created_on = DateTimeField()


class Provenance(BaseModel):
    id = AutoField()
    execution = ForeignKeyField(Execution, column_name="execution_id")
    namespace = ForeignKeyField(Namespace, column_name="namespace_id")
    from_text_id = CharField(index=True)
    from_text = TextField()

    class Meta:
        indexes = ((("namespace_id", "from_text_id"), True),)


class Content(BaseModel):
    id = AutoField()
    namespace = ForeignKeyField(Namespace, column_name="namespace_id")
    execution = ForeignKeyField(Execution, column_name="execution_id")
    provenance = ForeignKeyField(Execution, column_name="provenance_id")
    created_on = DateTimeField()
    anchor_text = TextField(null=True)
    anchor_markup = TextField(null=True)
    final_url = TextField()
    orig_url = TextField()
    link_brand_fqdn = CharField()
    path_slug = TextField()
    content_type = CharField(null=True)
    mime_type = CharField(null=True)
    mime_options = CharField(null=True)
    mime_maintype = CharField(null=True)
    mime_subtype = CharField(null=True)
    http_resp_headers = TextField()

    class Meta:
        indexes = ((("namespace_id", "provenance_id", "final_url"), True),)


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
            self.caches = config["caches"]
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


class CachedRequest(Model):
    orig_url = CharField(primary_key=True, index=True)
    http_status_code = IntegerField()
    http_response_url = TextField(index=True, null=True)
    http_response_headers = TextField(null=True)
    error_message = TextField(null=True)
    created_on = DateTimeField()
    content_type = CharField(null=True)
    mime_type = CharField(null=True)
    mime_options = CharField(null=True)
    mime_maintype = CharField(null=True)
    mime_subtype = CharField(null=True)

    class Meta:
        database = request_cache_db
        legacy_table_names = False

    def is_valid(self):
        return True if self.http_status_code == 200 else False

    def is_ignored(self):
        return True if self.http_status_code == -2 else False

    def cleaned(self, config):
        clean_furl = furl(self.http_response_url)
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


class RequestFactory:
    url_extractor = URLExtract()

    def __init__(self, config):
        self.config = config
        self.http = requests.Session()
        self.http.headers = {"User-Agent": "Lectio"}
        request_cache_db.init(
            config.caches["http_requests"]["db"],
            pragmas={"journal_mode": "wal", "cache_size": -1024 * 64, "busy_timeout": 5000},
        )
        request_cache_db.connect()
        request_cache_db.create_tables([CachedRequest])

    def parse(self, url):
        if self.config.ignore_url_patterns.match_any(url):
            return CachedRequest(orig_url=url, http_status_code=-2, message="Ignored")

        request = CachedRequest.get_or_none(CachedRequest.orig_url == url)
        if not request is None:
            return request
        try:
            resp = self.http.head(
                url, allow_redirects=True, timeout=self.config.http_request_timeout_secs,
            )
        except Exception as e:
            return CachedRequest(
                orig_url=url, http_status_code=-3, message="Exception during request: " + str(e)
            )

        if resp.status_code != 200:
            return CachedRequest(
                orig_url=url,
                http_status_code=-1,
                message="Invalid HTTP Status Code " + str(resp.status_code),
            )

        content_type = resp.headers.get("Content-Type")
        if not content_type is None:
            mime_type, mime_options = cgi.parse_header(content_type)
            mime_maintype, mime_subtype = mime_type.split("/")
        else:
            mime_type, mime_options, mime_maintype, mime_subtype = (None, None, None, None)
        request = CachedRequest(
            orig_url=url,
            http_response_url=resp.url,
            http_status_code=resp.status_code,
            http_response_headers=json.dumps(
                resp.headers,
                default=lambda o: o.__dict__ if hasattr(o, "__dict__") else str(o),
                sort_keys=True,
            ),
            content_type=content_type,
            mime_type=mime_type,
            mime_options=mime_options,
            mime_maintype=mime_maintype,
            mime_subtype=mime_subtype,
        )
        self.cache(request)
        return request

    def cache(self, request):
        for _ in range(0, DESTDB_LOCKED_RETRIES_COUNT):
            try:
                request.created_on = datetime.now()
                request.save(force_insert=True)
            except Exception as e:
                if "OperationalError: database is locked" in str(e):
                    time.sleep(1)  # wait for the lock to be freed
                    pass  # try again
                else:
                    print(
                        "RequestFactory.cache error: ", request, e,
                    )
                    raise e
            finally:
                break
        else:
            return False

    def close(self):
        request_cache_db.close()


class Origin:
    def __init__(self, config, row):
        self.row = row
        self.from_text_id = str(row[config.source["identify_urls_in_text_sql_col_index"]])
        self.from_text = row[config.source["extract_urls_from_text_sql_col_index"]]


@use("config", "source_data_db", "execution")
def consume_source_rows(config, source_data_db, execution):
    for row in source_data_db.execute_sql(config.source["rows_sql"]):
        yield Origin(config, list(row))


def extract_urls(origin):
    for url in RequestFactory.url_extractor.find_urls(origin.from_text, True):
        yield url, origin


@use("config", "link_factory")
def parse_urls(url, origin, config, link_factory):
    return url, origin, link_factory.parse(url)


def filter_ignore_urls(url, origin, link):
    if link.is_ignored():
        return False
    else:
        return url, origin, link


def filter_valid_urls(url, origin, link):
    if link.is_valid():
        return url, origin, link
    else:
        return False


@use("config", "execution", "namespace")
def save_content(url, origin, link, config, execution, namespace):
    final_url, link_brand_fqdn, path_slug = link.cleaned(config)
    # in case the database is locked due to concurrent writes,
    # keep trying before skipping the url
    for _ in range(0, DESTDB_LOCKED_RETRIES_COUNT):
        try:
            provenance, _ = Provenance.get_or_create(
                namespace_id=config.source["namespace"],
                from_text_id=str(origin.from_text_id),
                defaults={"from_text": origin.from_text, "execution": execution},
            )
            content, content_created = Content.get_or_create(
                namespace=namespace,
                provenance=provenance,
                final_url=final_url,
                defaults={
                    "execution": execution,
                    "link_brand_fqdn": link_brand_fqdn,
                    "orig_url": link.orig_url,
                    "path_slug": path_slug,
                    "created_on": datetime.now(),
                    "content_type": link.content_type,
                    "mime_type": link.mime_type,
                    "mime_options": link.mime_options,
                    "mime_maintype": link.mime_maintype,
                    "mime_subtype": link.mime_subtype,
                    "http_resp_headers": link.http_response_headers,
                },
            )
            yield content, content_created
        except Exception as e:
            if "OperationalError: database is locked" in str(e):
                time.sleep(1)  # wait for the lock to be freed
                pass  # try again
            else:
                print("Error: ", url, origin, link, e)
                raise e
        finally:
            break
    else:
        return False


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


def get_services(config, source_data_db, link_factory):
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
        "source_data_db": source_data_db,
        "link_factory": link_factory,
    }


if __name__ == "__main__":
    parser = bonobo.get_argument_parser()
    parser.add_argument("--config-url", action="store", required=True)

    with bonobo.parse_args(parser) as options:
        config = Configuration(**options)
        sourceDB = SqliteDatabase(
            config.source["db"],
            pragmas={"journal_mode": "wal", "cache_size": -1024 * 64, "query_only": True},
        )
        content_db.init(
            config.destination["db"],
            pragmas={"journal_mode": "wal", "cache_size": -1024 * 64, "busy_timeout": 5000},
        )
        content_db.connect()
        content_db.create_tables([Execution, Namespace, Provenance, Content])
        link_factory = RequestFactory(config)
        bonobo.run(get_graph(config), services=get_services(config, sourceDB, link_factory))
        link_factory.close()
        content_db.close()
        sourceDB.close()
