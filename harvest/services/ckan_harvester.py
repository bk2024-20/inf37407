# harvest/services/ckan_harvester.py
import datetime
import requests
from urllib.parse import urlencode
from django.db import transaction
from django.utils import timezone
from ..models import Source, Dataset, Resource, Tag, HarvestJob

CKAN_PAGE_ROWS_MAX = 1000  # CKAN tolère de grands rows; on restera raisonnable (ex: 100)

def _parse_dt(val):
    if not val:
        return None
    try:
        return datetime.datetime.fromisoformat(val.replace("Z","+00:00"))
    except Exception:
        return None

def _ensure_tags(dataset_obj, tag_list):
    names = [t.get("name") for t in (tag_list or []) if t.get("name")]
    if not names:
        return
    existing = {t.name: t for t in Tag.objects.filter(name__in=names)}
    to_add = []
    for name in names:
        obj = existing.get(name) or Tag.objects.create(name=name)
        to_add.append(obj)
    dataset_obj.tags.add(*to_add)

def _ckan_api_url(source):
    return source.base_url.rstrip("/") + source.api_path  # ex: .../api/3/action + /package_search

def _ckan_request(url, params, timeout=30):
    # simple GET avec gestion d’erreurs réseau
    resp = requests.get(url, params=params, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success", False):
        raise RuntimeError(f"CKAN returned success=false: {data}")
    return data.get("result") or {}

def _build_fq(organization=None, res_format=None, license_id=None, since_iso=None):
    """
    Construit un fq (filter query) CKAN, ex:
    - organization: "min-environnement"
    - res_format: "CSV"
    - license_id: "open-government-licence-canada"
    - since_iso (YYYY-MM-DD): metadata_modified:[2024-01-01T00:00:00Z TO *]
    """
    parts = []
    if organization:
        parts.append(f'organization:"{organization}"')
    if res_format:
        parts.append(f'res_format:"{res_format.upper()}"')
    if license_id:
        parts.append(f'license_id:"{license_id}"')
    if since_iso:
        parts.append(f'metadata_modified:[{since_iso}T00:00:00Z TO *]')
    return " ".join(parts) if parts else None

def harvest_ckan(source: Source, q="", organization=None, res_format=None, license_id=None,
                 since_iso=None, rows=100, max_pages=5):
    """
    Moissonne 'max_pages' de résultats (lecture seule).
    - q: requête plein texte
    - organization: identifiant org CKAN (pas le titre, le 'name')
    - res_format: CSV/JSON/… (non sensible à la casse)
    - license_id: ex: 'open-government-licence-canada'
    - since_iso: 'YYYY-MM-DD' pour filtrer par metadata_modified
    - rows: éléments par page
    """
    rows = min(max(rows, 1), 100)  # reste pragmatique
    url = _ckan_api_url(source)

    job = HarvestJob.objects.create(source=source, query=str({
        "q": q, "organization": organization, "res_format": res_format,
        "license_id": license_id, "since": since_iso, "rows": rows, "max_pages": max_pages
    }), status=HarvestJob.R)

    try:
        imported_total = 0
        found_total = 0

        for page in range(max_pages):
            start = page * rows
            fq = _build_fq(organization, res_format, license_id, since_iso)
            params = {"q": q, "rows": rows, "start": start}
            if fq:
                params["fq"] = fq

            result = _ckan_request(url, params)
            results = result.get("results") or []
            count = result.get("count") or 0
            found_total = count  # total global renvoyé par CKAN

            if not results:
                break

            with transaction.atomic():
                for pkg in results:
                    ds, _created = Dataset.objects.update_or_create(
                        source=source,
                        ckan_id=pkg.get("id",""),
                        defaults={
                            "name": pkg.get("name",""),
                            "title": pkg.get("title",""),
                            "notes": pkg.get("notes") or "",
                            "org": ((pkg.get("organization") or {}).get("title")) or "",
                            "license": pkg.get("license_title") or pkg.get("license_id") or "",
                            "spatial": (pkg.get("spatial") or pkg.get("geographies") or "")[:500],
                            "temporal_start": None,
                            "temporal_end": None,
                            "last_modified": _parse_dt(pkg.get("metadata_modified")),
                            "url": pkg.get("url") or "",
                        }
                    )
                    _ensure_tags(ds, pkg.get("tags"))

                    for res in pkg.get("resources") or []:
                        Resource.objects.update_or_create(
                            dataset=ds,
                            ckan_id=res.get("id",""),
                            defaults={
                                "name": res.get("name") or "",
                                "format": (res.get("format") or "").upper()[:50],
                                "url": res.get("url") or "",
                                "last_modified": _parse_dt(res.get("last_modified")),
                                "size": res.get("size") if isinstance(res.get("size"), int) else None,
                            }
                        )
                    imported_total += 1

            # Arrêt si on a dépassé le total
            if start + rows >= found_total:
                break

        job.found = found_total
        job.imported = imported_total
        job.status = HarvestJob.S
    except Exception as e:
        job.status = HarvestJob.F
        job.error = str(e)[:2000]
    finally:
        job.ended_at = timezone.now()
        job.save()

    return job
