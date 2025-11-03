"""
URL configuration for inf37407 project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, include
from rest_framework.routers import DefaultRouter
from rest_framework import permissions
from drf_yasg.views import get_schema_view
from drf_yasg import openapi
from harvest.views import DatasetViewSet
from harvest.views_stats import stats_view
from harvest.views_home import home_view
# GraphQL
from graphene_django.views import GraphQLView
from django.views.decorators.csrf import csrf_exempt
from harvest.schema import schema
from django.contrib.auth.decorators import login_required


router = DefaultRouter()
router.register(r"datasets", DatasetViewSet, basename="dataset")

schema_view = get_schema_view(
    openapi.Info(
        title="INF37407 – Harvest API",
        default_version="v1",
        description="API de consultation et moissonnage CKAN.",
    ),
    public=True,
    permission_classes=[permissions.AllowAny],
)

urlpatterns = [
    path("", home_view, name="home"),
    path("stats/", stats_view, name="stats"),
    path("admin/", admin.site.urls),
    path("api/", include(router.urls)),
    path("swagger/", schema_view.with_ui("swagger", cache_timeout=0), name="schema-swagger-ui"),
    path("redoc/", schema_view.with_ui("redoc", cache_timeout=0), name="schema-redoc"),
    path(
    "graphql/",
    login_required(
        csrf_exempt(
            GraphQLView.as_view(schema=schema, graphiql=True)
        )
    ),
),
]



