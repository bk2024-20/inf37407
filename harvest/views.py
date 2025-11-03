from django.shortcuts import render

# Create your views here.
from rest_framework import viewsets, filters
from .models import Dataset
from .serializers import DatasetSerializer

class DatasetViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Dataset.objects.select_related("source").prefetch_related("tags","resources").all()
    serializer_class = DatasetSerializer
    filter_backends = [filters.SearchFilter]
    search_fields = ["title", "org", "tags__name"]
