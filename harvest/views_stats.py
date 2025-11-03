from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.db.models import Count, Max
from .models import Dataset, Resource, Tag, HarvestJob

@login_required
def stats_view(request):
    total_datasets = Dataset.objects.count()
    total_resources = Resource.objects.count()
    total_tags = Tag.objects.count()
    last_harvest = HarvestJob.objects.aggregate(Max("ended_at"))["ended_at__max"]

    # Répartition par source (affichage avec barres par source OK)
    by_source = (
        Dataset.objects.values("source__name")
        .annotate(count=Count("id"))
        .order_by("-count")
    )
    max_source = max([x["count"] for x in by_source], default=1)
    by_source_rows = []
    for item in by_source:
        name = item["source__name"] or "Inconnue"
        count = item["count"]
        pct_of_max = round((count / max_source) * 100) if max_source else 0
        by_source_rows.append({
            "name": name,
            "count": count,
            "pct": pct_of_max,
            "style": f"width: {pct_of_max}%"
        })

    context = {
        "total_datasets": total_datasets,
        "total_resources": total_resources,
        "total_tags": total_tags,
        "last_harvest": last_harvest,
        "by_source_rows": by_source_rows,
    }
    return render(request, "harvest/stats.html", context)
