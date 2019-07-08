from django.conf.urls import url

from .viewsets import RecursiveAddRemove


urlpatterns = [
    url(r'docker/add-remove/$', RecursiveAddRemove.as_view({'post': 'create'})),
]
