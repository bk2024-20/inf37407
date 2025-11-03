import graphene
from graphene_django import DjangoObjectType
from .models import Dataset, Resource, Tag

class TagType(DjangoObjectType):
    class Meta:
        model = Tag
        fields = ("id", "name")

class ResourceType(DjangoObjectType):
    class Meta:
        model = Resource
        fields = ("id", "name", "format", "url", "last_modified", "size")

class DatasetType(DjangoObjectType):
    class Meta:
        model = Dataset
        fields = ("id","ckan_id","name","title","notes","org","license",
                  "spatial","temporal_start","temporal_end","last_modified","url",
                  "tags","resources","source")

class Query(graphene.ObjectType):
    datasets = graphene.List(
        DatasetType,
        search=graphene.String(required=False)
    )
    dataset = graphene.Field(DatasetType, id=graphene.Int(required=True))

    def resolve_datasets(root, info, search=None):
        qs = Dataset.objects.select_related("source").prefetch_related("tags","resources").all()
        if search:
            qs = qs.filter(title__icontains=search) | qs.filter(org__icontains=search) | qs.filter(tags__name__icontains=search)
        return qs.distinct()

    def resolve_dataset(root, info, id):
        return Dataset.objects.select_related("source").prefetch_related("tags","resources").get(id=id)

schema = graphene.Schema(query=Query)
