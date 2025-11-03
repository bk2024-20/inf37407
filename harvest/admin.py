from django.contrib import admin

# Register your models here.
from django.contrib import admin
from .models import Source, Dataset, Resource, Tag, HarvestJob

@admin.register(Source)
class SourceAdmin(admin.ModelAdmin):
    list_display = ("name", "base_url", "active")
    search_fields = ("name",)
    list_filter = ("active",)

@admin.register(Tag)
class TagAdmin(admin.ModelAdmin):
    list_display = ("name",)
    search_fields = ("name",)

class ResourceInline(admin.TabularInline):
    model = Resource
    extra = 0

@admin.register(Dataset)
class DatasetAdmin(admin.ModelAdmin):
    list_display = ("title", "source", "org", "license", "last_modified")
    list_filter = ("source", "license")
    search_fields = ("title", "org", "tags__name")
    inlines = [ResourceInline]
    filter_horizontal = ("tags",)

@admin.register(HarvestJob)
class HarvestJobAdmin(admin.ModelAdmin):
    list_display = ("source", "status", "started_at", "ended_at", "found", "imported")
    list_filter = ("source", "status")
    search_fields = ("query",)
