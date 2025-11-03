import requests
from urllib.parse import urljoin
from django.db import transaction
from django.utils import timezone
from ..models import Source, Dataset, Resource, Tag, HarvestJob

def _get_json(url, params=None, timeout=30):
    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def harvest_dataverse(
    source: Source,
    q: str | None = None,
    per_page: int = 20,
    max_pages: int = 2,
    subtree: str | None = None,
):
    """
    Moissonne Borealis (Dataverse) en lecture seule.
    - source.base_url attendu: https://borealisdata.ca/api
    - q: mot-clé; si None ou "", on cherche avec "*"
    - subtree: alias du dataverse (ex: "daviddeslauriers")
    """
    q = q or "*"  # <-- important: retourne "tout" si pas de mot-clé
    job = HarvestJob.objects.create(
        source=source,
        query=str({"q": q, "per_page": per_page, "max_pages": max_pages, "subtree": subtree}),
        status=HarvestJob.R
    )
    debug_lines: list[str] = []
    try:
        imported = 0
        total_found = 0

        search_url = urljoin(source.base_url.rstrip("/") + "/", "search")
        debug_lines.append(f"SEARCH {search_url}")

        # Dataverse utilise 'start' (offset), pas 'page'
        for i in range(max_pages):
            start = i * per_page
            params = {
                "q": q,
                "type": "dataset",
                "per_page": per_page,
                "start": start,  # <-- clé correcte
            }
            if subtree:
                params["subtree"] = subtree  # limiter à un dataverse
            debug_lines.append(f"params[{i}]: {params}")

            data = _get_json(search_url, params=params)
            data_block = data.get("data") or {}
            items = data_block.get("items") or []
            if i == 0:
                total_found = int(data_block.get("total_count") or 0)
                debug_lines.append(f"total_found={total_found}")

            if not items:
                debug_lines.append("no items; break")
                break

            with transaction.atomic():
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

                    # ... au même endroit qu'avant, dans la boucle des items ...

                    # Fichiers publiés (latest-published) pour éviter les 403/embargos
                    files_url = urljoin(
                        source.base_url.rstrip("/") + "/",
                        "datasets/:persistentId/versions/:latest-published/files"
                    )
                    fdata = _get_json(files_url, params={"persistentId": pid})

                    # Dataverse renvoie ▶ {"status":"OK","data":[ ... ]} ◀
                    raw = fdata.get("data", [])
                    files = raw if isinstance(raw, list) else (raw.get("files") or [])

                    for f in files:
                        # Structure typique d’un item de liste "files":
                        # { "label": "...", "dataFile": { "id": 123, "filesize": 456, "contentType": "text/csv", "persistentId": "doi:..." }, ... }
                        label = f.get("label") or ""
                        df = f.get("dataFile") or {}
                        fid = df.get("id")
                        size = df.get("filesize")
                        content_type = (df.get("contentType") or "")
                        file_pid = df.get("persistentId") or pid  # lien persistant du fichier; à défaut, on garde le pid du dataset

                        Resource.objects.update_or_create(
                            dataset=ds,
                            ckan_id=str(fid),  # on stocke l'id de dataFile
                            defaults={
                                "name": label,
                                "format": (content_type.split("/")[-1].upper()[:50] if content_type else ""),
                                "url": file_pid,
                                "last_modified": None,
                                "size": size if isinstance(size, int) else None,
                            }
                        )

                    imported += 1

            # stop si on a atteint le total
            if start + per_page >= total_found:
                debug_lines.append("reached total; stop")
                break

        job.found = total_found
        job.imported = imported
        job.status = HarvestJob.S
    except Exception as e:
        job.status = HarvestJob.F
        debug_lines.append(f"ERR: {e}")
        job.error = ("\n".join(debug_lines))[:2000]
    finally:
        job.ended_at = timezone.now()
        # ajoute un peu de debug aussi en succès
        if not job.error and debug_lines:
            job.error = ("\n".join(debug_lines))[:2000]
        job.save()
    return job
