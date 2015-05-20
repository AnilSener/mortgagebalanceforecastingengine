from django.conf.urls import patterns, include, url
from django.contrib import admin
from forecastingengine import views

urlpatterns = patterns('',
    # Examples:
    # url(r'^$', 'jpmorgan.views.home', name='home'),
    # url(r'^blog/', include('blog.urls')),

    url(r'^admin/', include(admin.site.urls)),
    url(r'^forecastingengine/$',views.calculator_view,name='calculator_view'),
)
