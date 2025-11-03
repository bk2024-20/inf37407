from rest_framework import serializers
from .models import Dataset, Resource, Tag

class TagSerializer(serializers.ModelSerializer):
    class Meta:
        model = Tag
        fields = ["id", "name"]

class ResourceSerializer(serializers.ModelSerializer):
    class Meta:
        model = Resource
        fields = ["id", "name", "format", "url", "last_modified", "size"]

class DatasetSerializer(serializers.ModelSerializer):
    tags = TagSerializer(many=True, read_only=True)
    resources = ResourceSerializer(many=True, read_only=True)
    class Meta:
        model = Dataset
        fields = ["id","source","ckan_id","name","title","notes","org","license",
                  "spatial","temporal_start","temporal_end","last_modified","url",
                  "tags","resources"]
