from django.core.management.base import BaseCommand, CommandError
from harvest.models import Source
from harvest.services.dataverse_harvester import harvest_dataverse

class Command(BaseCommand):
    help = "Moissonne Borealis (Dataverse) via /api/search"

    def add_arguments(self, parser):
        parser.add_argument("--source", required=True, help='Nom de la Source (ex: "Borealis")')
        parser.add_argument("--q", default="", help="Mots-clés (ex: 'eau'); vide = '*' (tout)")
        parser.add_argument("--per_page", type=int, default=20)
        parser.add_argument("--max_pages", type=int, default=2)
        parser.add_argument("--subtree", default=None, help="Alias du dataverse (ex: daviddeslauriers)")

    def handle(self, *args, **opts):
        name = opts["source"]
        try:
            src = Source.objects.get(name=name, active=True)
        except Source.DoesNotExist:
            raise CommandError(f"Source active introuvable: {name}")

        job = harvest_dataverse(
            source=src,
            q=opts.get("q") or None,
            per_page=opts["per_page"],
            max_pages=opts["max_pages"],
            subtree=opts["subtree"],
        )
        status = job.get_status_display()
        msg = f"{src.name} -> Job {job.id} status={status} found={job.found} imported={job.imported}"
        if job.error:
            msg += f"\n{job.error}"
        self.stdout.write(msg)
