from django.core.management.base import BaseCommand, CommandError
from harvest.models import Source
from harvest.services.ckan_harvester import harvest_ckan

CKAN_PATH = "/package_search"  # signature d'une source CKAN

class Command(BaseCommand):
    help = "Moissonne une ou plusieurs sources CKAN via /package_search (ignore les sources non-CKAN)."

    def add_arguments(self, parser):
        parser.add_argument("--source", help='Nom EXACT de la Source (ex: "OpenGouv"). Omettre pour toutes les sources CKAN actives.')
        parser.add_argument("--q", default="", help="Requête plein texte (ex: 'eau OR hydrologie')")
        parser.add_argument("--organization", default=None, help="Identifiant org CKAN (name, pas title)")
        parser.add_argument("--res_format", default=None, help="Filtrer par format de ressource (ex: CSV)")
        parser.add_argument("--license_id", default=None, help="Filtrer par license_id")
        parser.add_argument("--since", dest="since_iso", default=None, help="YYYY-MM-DD (metadata_modified >= date)")
        parser.add_argument("--rows", type=int, default=50, help="Résultats par page (<=100)")
        parser.add_argument("--max_pages", type=int, default=2, help="Nombre de pages à récupérer")

    def handle(self, *args, **opts):
        src_name = opts.get("source")

        # Base: sources actives
        qs = Source.objects.filter(active=True)

        if src_name:
            qs = qs.filter(name=src_name)
            if not qs.exists():
                raise CommandError(f"Aucune source active nommée '{src_name}'.")
            # Sûreté: si on a ciblé une source non-CKAN, on la skippe
            for src in list(qs):
                if (src.api_path or "").strip().lower() != CKAN_PATH:
                    self.stdout.write(self.style.WARNING(f"Skip non-CKAN source: {src.name} (api_path={src.api_path!r})"))
                    qs = qs.exclude(pk=src.pk)
        else:
            # Sans --source, ne garder QUE les CKAN
            qs = qs.filter(api_path=CKAN_PATH)

        if not qs.exists():
            raise CommandError("Aucune source CKAN à moissonner (api_path=/package_search).")

        for src in qs:
            self.stdout.write(self.style.HTTP_INFO(f"--> Harvest {src.name} (CKAN)"))

            job = harvest_ckan(
                source=src,
                q=opts["q"],
                organization=opts["organization"],
                res_format=opts["res_format"],
                license_id=opts["license_id"],
                since_iso=opts["since_iso"],
                rows=opts["rows"],
                max_pages=opts["max_pages"],
            )

            status = job.get_status_display()
            msg = f"{src.name} -> Job {job.id} status={status} found={job.found} imported={job.imported}"
            if job.error:
                self.stdout.write(self.style.WARNING(msg + f" | error={job.error[:140]}..."))
            else:
                self.stdout.write(self.style.SUCCESS(msg))
