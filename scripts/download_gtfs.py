"""Download GTFS data for mainland Okinawa bus operators from OTTOP API"""

import io
import zipfile
from pathlib import Path

import click
import requests
from loguru import logger

AGENCIES_URL = "https://api3.ottop.org/agencies"
DOWNLOAD_URL = "https://api3.ottop.org/download/gtfs/ooXuXei4op7y/{code}"
REQUEST_TIMEOUT = 60

# Mainland Okinawa bus agencies (whitelist)
MAINLAND_AGENCIES = {
    "1000020470007_1",  # 沖縄県観光振興課
    "1360001007585",    # 平安座総合開発（株）
    "1360003007856",    # 合同会社やんばる急行バス
    "2360001000457",    # 沖縄バス（株）
    "2360001015810",    # 東陽バス（株）
    "3000020472158",    # Ｎバス 南城市
    "360005005779",     # 沖縄エアポートシャトルLLP
    "5000020472107",    # 糸満市
    "5000020472115",    # 沖縄市循環バス
    "5000020472131",    # うるま市
    "5000020473014",    # 国頭村役場
    "5000020473278",    # 北中城村コミュニティバス
    "5000020473286",    # 中城村
    "7000020473243",    # 読谷村
    "7011501003070",    # 東京バス株式会社
    "8290002032402",    # 沖東交通グループ
    "1000020472093",    # 名護市
    "4000020473031",    # 東村
}


def _fetch_agencies() -> list[dict]:
    """Fetch all agencies from OTTOP API

    Returns:
        list[dict]: All agencies from the API
    """

    resp = requests.get(AGENCIES_URL, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()

    return resp.json()["agencies"]


def download(out_dir: str) -> list[Path]:
    """Download and extract GTFS zips for mainland Okinawa into out_dir

    Args:
        out_dir (str): Directory to extract GTFS feeds into

    Returns:
        list[Path]: List of paths to extracted GTFS directories
    """

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    all_agencies = _fetch_agencies()
    allowed_agencies = [a for a in all_agencies if a["code"] in MAINLAND_AGENCIES]
    skipped_agencies = [a for a in all_agencies if a["code"] not in MAINLAND_AGENCIES]
    for ag in skipped_agencies:
        logger.debug("Skipped: {} ({}), region={}", ag["name"], ag["code"], ag.get("region", "?"))

    extracted: list[Path] = []
    for ag in allowed_agencies:
        code = ag["code"]
        name = ag["name"]
        url = DOWNLOAD_URL.format(code=code)

        logger.info("Downloading {} ({})...", name, code)
        resp = requests.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()

        # Extract zip
        dest = out / code
        dest.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            zf.extractall(dest)

        logger.success("Downloaded {} — {:.0f} KB", name, len(resp.content) / 1024)
        extracted.append(dest)
    logger.info("Done — downloaded {}, skipped {}", len(extracted), len(skipped_agencies))

    return extracted


@click.command()
@click.option(
    "--out-dir",
    default="gtfs",
    show_default=True,
    help="Directory to extract GTFS feeds into",
)
def main(out_dir: str) -> None:
    """Download GTFS data for all mainland Okinawa bus operators"""
    download(out_dir)


if __name__ == "__main__":
    main()
