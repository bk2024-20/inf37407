from django.core.management.base import BaseCommand, CommandError
from harvest.models import Source
from harvest.services.ckan_harvester import harvest_ckan

class Command(BaseCommand):
    help = "Moissonne une ou toutes les sources CKAN via package_search"

    def add_arguments(self, parser):
        parser.add_argument("--source", help='Nom exact de la Source (ex: "OpenGouv"). Omettre pour toutes les sources actives.')
        parser.add_argument("--q", default="", help="Requête plein texte (ex: 'eau OR hydrologie')")
        parser.add_argument("--organization", default=None, help="Identifiant org CKAN (name, pas title)")
        parser.add_argument("--res_format", default=None, help="Filtrer par format de ressource (ex: CSV)")
        parser.add_argument("--license_id", default=None, help="Filtrer par license_id")
        parser.add_argument("--since", dest="since_iso", default=None, help="YYYY-MM-DD (metadata_modified >= date)")
        parser.add_argument("--rows", type=int, default=50, help="Résultats par page (<=100)")
        parser.add_argument("--max_pages", type=int, default=2, help="Nombre de pages à récupérer")

    def handle(self, *args, **opts):
        src_name = opts.get("source")
        qs = Source.objects.filter(active=True)
        if src_name:
            qs = qs.filter(name=src_name)

        if not qs.exists():
            raise CommandError("Aucune source active correspondante.")

        for src in qs:
            self.stdout.write(self.style.HTTP_INFO(f"--> Harvest {src.name}"))
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
            self.stdout.write(self.style.SUCCESS(
                f"{src.name} -> Job {job.id} status={status} found={job.found} imported={job.imported}"
            ))
            if job.error:
                self.stdout.write(self.style.WARNING(job.error))
