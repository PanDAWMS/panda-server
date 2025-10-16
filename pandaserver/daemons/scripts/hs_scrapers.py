import sys

import polars as pl
import requests
from bs4 import BeautifulSoup
from pandacommon.pandalogger import logger_utils
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.thread_utils import GenericThread

from pandaserver.config import panda_config
from pandaserver.srvcore.CoreUtils import normalize_cpu_model

main_logger = PandaLogger().getLogger("hs_scrapers")

DEFAULT_URL_SL6 = "https://w3.hepix.org/benchmarking/sl6-x86_64-gcc44.html"
DEFAULT_URL_SL7 = "https://w3.hepix.org/benchmarking/sl7-x86_64-gcc48.html"
DEFAULT_URL_HS23 = "https://raw.githubusercontent.com/HEPiX-Forum/hepix-forum.github.io/master/_data/HS23scores.csv"


def _parse_cores(val: str) -> tuple[int | None, int]:
    smt = 1 if ("HT on" in val or "SMT on" in val) else 0
    tokens = val.replace("(", " ").split()
    ncores = next((int(t) for t in tokens if t.isdigit()), None)
    return ncores, smt


# ---------------------- HTML scrapers (SL6/SL7) ----------------------
class BaseHS06Scraper:
    HEADERS = [
        "CPU",
        "HS06",
        "Clock speed (MHz)",
        "L2+L3 cache size (grand total, KB)",
        "Cores (runs)",
        "Memory (GB)",
        "Mainboard type",
        "Site",
    ]

    def __init__(self, task_buffer, url: str):
        self.url = url
        self.session = requests.Session()
        self.task_buffer = task_buffer

    def run(self) -> None:
        html = self._fetch_html(self.url)
        df = self._parse_html_to_polars(html)
        self._insert(df)

    def _insert_cpu_perf_rows(self, rows: list[dict], source_url: str) -> None:
        sql = (
            "INSERT INTO atlas_panda.cpu_benchmarks "
            "(cpu_type, cpu_type_normalized, smt_enabled, ncores, site, score_per_core, source) "
            "VALUES (:cpu_type, :cpu_type_normalized, :smt_enabled, :ncores, :site, :score_per_core, :source)"
        )
        for r in rows:
            r["source"] = source_url
            r["cpu_type_normalized"] = normalize_cpu_model(r["cpu_type"])
            status, res = self.task_buffer.querySQLS(sql, r)

    def _fetch_html(self, url: str) -> str:
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.text

    def _parse_html_to_polars(self, html: str) -> pl.DataFrame:
        soup = BeautifulSoup(html, "html.parser")
        h2 = soup.find("h2", {"id": "benchmark-results"})
        if not h2:
            raise RuntimeError("Benchmark results heading not found.")
        tables = h2.find_all_next("table", class_="striped")
        if not tables:
            raise RuntimeError("No benchmark results table found after the heading.")
        table = tables[0]

        # Build rows -> Polars
        rows = []
        for tr in table.find_all("tr")[1:]:
            cols = [td.get_text(strip=True) for td in tr.find_all("td")]
            if not any(cols):
                continue
            rows.append(cols)

        # Use fixed headers for SL6; SL7 will override to read <th>
        df = pl.DataFrame(rows, schema=self.HEADERS)
        df = (
            df.select(
                pl.col("CPU").alias("cpu_type"),
                pl.col("HS06").cast(pl.Float64, strict=False).alias("score"),
                pl.col("Cores (runs)").alias("cores"),
                pl.col("Site").alias("site"),
            )
            .with_columns(
                pl.col("cores").map_elements(lambda s: _parse_cores(s)[0], return_dtype=pl.Int64).alias("ncores"),
                pl.col("cores").map_elements(lambda s: _parse_cores(s)[1], return_dtype=pl.Int64).alias("smt_enabled"),
            )
            .with_columns((pl.col("score") / pl.col("ncores")).alias("score_per_core"))
            .drop(["cores", "score"])
            .drop_nulls(["ncores", "score_per_core"])
        )
        return df

    def _insert(self, df: pl.DataFrame) -> None:
        self._insert_cpu_perf_rows(df.to_dicts(), self.url)


class HS06ScraperSL6(BaseHS06Scraper):
    def __init__(self, task_buffer, url: str = DEFAULT_URL_SL6):
        super().__init__(task_buffer, url)


class HS06ScraperSL7(BaseHS06Scraper):
    def __init__(self, task_buffer, url: str = DEFAULT_URL_SL7):
        super().__init__(task_buffer, url)

    # SL7 table provides <th> headers; reuse logic but rebuild schema from them.
    def _parse_html_to_polars(self, html: str) -> pl.DataFrame:
        soup = BeautifulSoup(html, "html.parser")
        h2 = soup.find("h2", {"id": "benchmark-results"})
        if not h2:
            raise RuntimeError("Benchmark results heading not found.")
        tables = h2.find_all_next("table", class_="striped")
        if not tables:
            raise RuntimeError("No benchmark results table found after the heading.")
        table = tables[0]

        headers = [th.get_text(strip=True) for th in table.find_all("th")]
        rows = []
        for tr in table.find_all("tr")[1:]:
            cols = [td.get_text(strip=True) for td in tr.find_all("td")]
            if not any(cols):
                continue
            rows.append(cols)

        df = pl.DataFrame(rows, schema=headers)
        df = (
            df.select(
                pl.col("CPU").alias("cpu_type"),
                pl.col("HS06").cast(pl.Float64, strict=False).alias("score"),
                pl.col("Cores (runs)").alias("cores"),
                pl.col("Site").alias("site"),
            )
            .with_columns(
                pl.col("cores").map_elements(lambda s: _parse_cores(s)[0], return_dtype=pl.Int64).alias("ncores"),
                pl.col("cores").map_elements(lambda s: _parse_cores(s)[1], return_dtype=pl.Int64).alias("smt_enabled"),
            )
            .with_columns((pl.col("score") / pl.col("ncores")).alias("score_per_core"))
            .drop(["cores", "score"])
            .drop_nulls(["ncores", "score_per_core"])
        )
        return df


# ---------------------- HS23 CSV ingestor (Polars) ----------------------
class HS23Ingestor:
    def __init__(self, task_buffer, url: str = DEFAULT_URL_HS23):
        self.url = url
        self.logger = logger_utils.make_logger(main_logger, "HS23Ingestor")
        self.task_buffer = task_buffer

    def run(self) -> None:
        max_timestamp = self._select_max_timestamp(None)
        df = self._fetch()
        df = self._transform(df, max_timestamp)
        self._insert(df)

    def _fetch(self) -> pl.DataFrame:
        df = pl.read_csv(self.url, ignore_errors=True)
        return df

    def _transform(self, df: pl.DataFrame, max_timestamp) -> pl.DataFrame:
        out = (
            df.select(
                pl.col("CPU").alias("cpu_type"),
                pl.col("SMT enabled").alias("smt_enabled"),
                pl.col("# Sockets").alias("sockets"),
                pl.col("Cores/Socket").alias("cores_per_socket"),
                pl.col("Ncores").alias("ncores"),
                pl.col("Site").alias("site"),
                pl.col("Score/Ncores").alias("score_per_core"),
                pl.col("last_date").alias("timestamp"),
            )
            .with_columns(
                pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False),
                # normalize SMT to 0/1
                pl.when(pl.col("smt_enabled").cast(pl.Utf8, strict=False).str.to_lowercase().is_in(["1", "true", "yes", "on"]))
                .then(pl.lit(1))
                .otherwise(
                    pl.when(pl.col("smt_enabled").cast(pl.Utf8, strict=False).str.to_lowercase().is_in(["0", "false", "no", "off"]))
                    .then(pl.lit(0))
                    .otherwise(pl.col("smt_enabled").cast(pl.Int64, strict=False).fill_null(0))
                )
                .alias("smt_enabled"),
                pl.col("sockets").cast(pl.Int64, strict=False),
                pl.col("cores_per_socket").cast(pl.Int64, strict=False),
                pl.col("ncores").cast(pl.Int64, strict=False),
                pl.col("score_per_core").cast(pl.Float64, strict=False),
            )
            .filter(pl.col("timestamp") > max_timestamp)
        )
        return out

    def _select_max_timestamp(self, df: pl.DataFrame) -> None:
        sql = "SELECT max(timestamp) FROM atlas_panda.cpu_benchmarks"
        status, res = self.task_buffer.querySQLS(sql, {})
        max_timestamp = res[0][0]
        if not max_timestamp:
            max_timestamp = pl.datetime(1970, 1, 1)
        return max_timestamp

    def _insert(self, df: pl.DataFrame) -> None:
        sql = (
            "INSERT INTO atlas_panda.cpu_benchmarks "
            "(cpu_type, cpu_type_normalized, smt_enabled, sockets, cores_per_socket, ncores, site, "
            "score_per_core, timestamp, source) "
            "VALUES (:cpu_type, :cpu_type_normalized, :smt_enabled, :sockets, :cores_per_socket, :ncores, "
            ":site, :score_per_core, :timestamp, :source)"
        )
        for row in df.to_dicts():
            row["source"] = self.url
            row["cpu_type_normalized"] = normalize_cpu_model(row["cpu_type"])
            _, _ = self.task_buffer.querySQLS(sql, row)


def main(tbuf=None, **kwargs):
    requester_id = GenericThread().get_full_id(__name__, sys.modules[__name__].__file__)

    # instantiate TB
    if tbuf is None:
        from pandaserver.taskbuffer.TaskBuffer import taskBuffer

        taskBuffer.init(
            panda_config.dbhost,
            panda_config.dbpasswd,
            nDBConnection=1,
            useTimeout=True,
            requester=requester_id,
        )
    else:
        taskBuffer = tbuf

    try:
        main_logger.debug("Start")
        # We assume the SL6 and SL7 pages don't change and do not need to be updated.
        # In case you re-run the scripts, be aware that it will cause duplicate entries.
        # main_logger.debug("Starting HS06 SL6 scraper...")
        # HS06ScraperSL6(taskBuffer).run()
        # main_logger.debug("Starting HS06 SL7 scraper...")
        # HS06ScraperSL7(taskBuffer).run()

        main_logger.debug("Starting HS23 ingestor...")
        HS23Ingestor(taskBuffer).run()
        main_logger.debug("Done.")
    except Exception as e:
        main_logger.error(f"Error: {e}")
    finally:
        # stop the taskBuffer if it was created inside this script
        if tbuf is None:
            taskBuffer.cleanup(requester=requester_id)


# ---------------------- main ----------------------
if __name__ == "__main__":
    main()
