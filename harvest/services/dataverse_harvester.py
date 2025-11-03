# harvest/services/dataverse_harvester.py
import requests
from requests import HTTPError
from urllib.parse import urljoin
from django.db import transaction
from django.utils import timezone
from ..models import Source, Dataset, Resource, HarvestJob

HEADERS = {
    "User-Agent": "INF37407-harvest/1.0 (+https://example.com)",
    "Accept": "application/json",
}

def _get_json(url, params=None, timeout=30):
    r = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.json()

def _search_dataverse(search_url, params):
    r = requests.get(search_url, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()

def _upsert_items_and_files(source, items):
    """Crée/MAJ Datasets + Resources pour une liste d'items Dataverse."""
    count_imported = 0
    for it in items:
        title = it.get("name") or ""
        pid = it.get("global_id") or it.get("identifier") or ""   # doi:... ou handle
        url = it.get("url") or ""
        ds, _ = Dataset.objects.update_or_create(
            source=source,
            ckan_id=pid,
            defaults={
                "name": pid,
                "title": title,
                "notes": "",
                "org": (it.get("publisher") or ""),
                "license": "",
                "spatial": "",
                "temporal_start": None,
                "temporal_end": None,
                "last_modified": None,
                "url": url,
            }
        )
        # fichiers publiés uniquement
        files_url = urljoin(source.base_url.rstrip("/") + "/", "datasets/:persistentId/versions/:latest-published/files")
        fdata = _get_json(files_url, params={"persistentId": pid})
        raw = fdata.get("data", [])
        files = raw if isinstance(raw, list) else (raw.get("files") or [])
        for f in files:
            df = f.get("dataFile") or {}
            fid = df.get("id")
            size = df.get("filesize")
            ctype = (df.get("contentType") or "")
            file_pid = df.get("persistentId") or pid
            Resource.objects.update_or_create(
                dataset=ds,
                ckan_id=str(fid),
                defaults={
                    "name": f.get("label") or "",
                    "format": (ctype.split("/")[-1].upper()[:50] if ctype else ""),
                    "url": file_pid,
                    "last_modified": None,
                    "size": size if isinstance(size, int) else None,
                }
            )
        count_imported += 1
    return count_imported

def harvest_dataverse(source: Source, q: str | None = None, per_page: int = 20, max_pages: int = 2, subtree: str | None = None):
    """
    Moissonne Borealis (Dataverse) en lecture seule.
    - source.base_url attendu: https://borealisdata.ca/api
    - q: mot-clé; si vide -> "*" (tout)
    - subtree: alias d’un dataverse (ex: "daviddeslauriers")
    """
    q = q or "*"
    job = HarvestJob.objects.create(
        source=source,
        query=str({"q": q, "per_page": per_page, "max_pages": max_pages, "subtree": subtree}),
        status=HarvestJob.R
    )
    debug = []
    try:
        imported = 0
        total_found = 0
        search_url = urljoin(source.base_url.rstrip("/") + "/", "search")
        debug.append(f"SEARCH {search_url}")

        # -------- essai 1 : avec subtree si fourni --------
        params_base = {"q": q, "type": "dataset", "per_page": per_page}
        if subtree:
            params_base["subtree"] = subtree

        try:
            for i in range(max_pages):
                params = dict(params_base, start=i * per_page)
                debug.append(f"params[{i}]: {params}")
                data = _search_dataverse(search_url, params)
                data_block = data.get("data") or {}
                items = data_block.get("items") or []
                if i == 0:
                    total_found = int(data_block.get("total_count") or 0)
                    debug.append(f"total_found={total_found}")
                if not items:
                    break
                imported += _upsert_items_and_files(source, items)
                if (i + 1) * per_page >= total_found:
                    break

        except HTTPError as e:
            # -------- fallback : si 403 avec subtree -> relance sans subtree + filtre local --------
            if subtree and e.response is not None and e.response.status_code == 403:
                debug.append("403 with subtree -> fallback WITHOUT subtree, then local filter by URL")
                imported = 0
                total_found = 0
                for i in range(max_pages):
                    params = {"q": q, "type": "dataset", "per_page": per_page, "start": i * per_page}
                    debug.append(f"fallback_params[{i}]: {params}")
                    data = _search_dataverse(search_url, params)
                    data_block = data.get("data") or {}
                    items = data_block.get("items") or []
                    if i == 0:
                        total_found = int(data_block.get("total_count") or 0)
                        debug.append(f"fallback_total_found={total_found}")
                    if not items:
                        break
                    subfrag = f"/dataverse/{subtree}"
                    items = [it for it in items if subfrag in (it.get("url") or "")]
                    if not items:
                        continue
                    imported += _upsert_items_and_files(source, items)
            else:
                raise

        job.found = total_found
        job.imported = imported
        job.status = HarvestJob.S

    except Exception as e:
        job.status = HarvestJob.F
        debug.append(f"ERR: {e}")
        job.error = "\n".join(debug)[:2000]
    finally:
        job.ended_at = timezone.now()
        if not job.error:
            job.error = "\n".join(debug)[:2000]
        job.save()
    return job
