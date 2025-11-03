from django.db import models

# Create your models here.
from django.db import models

class Source(models.Model):
    name = models.CharField(max_length=100, unique=True)  # OpenGouv, CanWin, Données Québec, Boréalis
    base_url = models.URLField()                          # ex: https://<host>/api/3/action
    api_path = models.CharField(max_length=200, default="/package_search")
    active = models.BooleanField(default=True)
    def __str__(self): return self.name

class Tag(models.Model):
    name = models.CharField(max_length=100, unique=True)
    def __str__(self): return self.name

class Dataset(models.Model):
    source = models.ForeignKey(Source, on_delete=models.CASCADE, related_name="datasets")
    ckan_id = models.CharField(max_length=200, db_index=True)
    name = models.CharField(max_length=255)              # slug / name CKAN
    title = models.CharField(max_length=500, blank=True)
    notes = models.TextField(blank=True)                 # description
    org = models.CharField(max_length=255, blank=True)   # producteur/org
    license = models.CharField(max_length=200, blank=True)
    spatial = models.CharField(max_length=500, blank=True)
    temporal_start = models.DateField(null=True, blank=True)
    temporal_end = models.DateField(null=True, blank=True)
    last_modified = models.DateTimeField(null=True, blank=True)
    tags = models.ManyToManyField(Tag, blank=True)
    url = models.URLField(max_length=1000, blank=True, default="")   # 
    created_at = models.DateTimeField(auto_now_add=True)
    class Meta:
        unique_together = ("source", "ckan_id")
    def __str__(self): return f"{self.source.name} • {self.title or self.name}"

class Resource(models.Model):
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE, related_name="resources")
    ckan_id = models.CharField(max_length=200, db_index=True)
    name = models.CharField(max_length=500, blank=True)
    format = models.CharField(max_length=50, blank=True) # CSV, JSON, SHP…
    url = models.URLField(max_length=1000, blank=True, default="")   # ↑
    last_modified = models.DateTimeField(null=True, blank=True)
    size = models.BigIntegerField(null=True, blank=True)
    class Meta:
        unique_together = ("dataset", "ckan_id")

class HarvestJob(models.Model):
    P, R, S, F = "P","R","S","F"
    STATUS_CHOICES = [(P,"Pending"),(R,"Running"),(S,"Success"),(F,"Failed")]
    source = models.ForeignKey(Source, on_delete=models.CASCADE, related_name="jobs")
    query = models.TextField()                           # filtres (mots-clés, org, bbox…)
    started_at = models.DateTimeField(auto_now_add=True)
    ended_at = models.DateTimeField(null=True, blank=True)
    status = models.CharField(max_length=1, choices=STATUS_CHOICES, default=P)
    found = models.IntegerField(default=0)
    imported = models.IntegerField(default=0)
    error = models.TextField(blank=True)
    def __str__(self): return f"{self.source.name} [{self.get_status_display()}] {self.started_at:%Y-%m-%d %H:%M}"
