from django.contrib import admin
from django.urls import path

from example.demoapp import views

urlpatterns = [
    path("admin/", admin.site.urls),
    path("", views.index),
    path("enqueue/via-code/<str:name>/", views.enqueue_via_code),
    path("enqueue/via-decorator/<str:user_id>/", views.enqueue_via_decorator),
    path("result/<str:task_id>/", views.task_result),
]
