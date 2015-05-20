from django.db import models
from datetime import datetime
from calculator import forecastBalance
# Model for Macro Economic Data inputs and Balance Estimations of each user
class MacroEcon(models.Model):
    id=models.AutoField(primary_key=True)
    Date = models.DateField(verbose_name='Start Date')
    HPI_NY = models.FloatField()
    HPI_CA = models.FloatField()
    MTG = models.FloatField()
    userID = models.IntegerField(verbose_name='User ID')
    isForecast=models.BooleanField(verbose_name='Is Forecast?')
    estimation = models.DecimalField(decimal_places=4,max_digits=30,verbose_name='Expected Balance')

# Model for Forecasts and Simulations, bth are also triggered by the instantioation of this model
class Forecast(models.Model):
    startTime=models.DateTimeField()
    endTime=models.DateTimeField()
    status=models.BooleanField()
    def __init__(self,macro_econ_qs,request,type):
        self.startTime=datetime.now()
        self.status=True
        try:
            print request.GET
            self.endTime=forecastBalance(macro_econ_qs,request,type)
        except Exception:
            print Exception.message
            self.status=False
#Model for Django Tables 2 application which used to display the results
import django_tables2 as tables
class ForecastResultsTable(tables.Table):
    class Meta:
        model = MacroEcon
        # add class="paleblue" to <table> tag
        attrs = {"class": "paleblue"}

#Just to add one dummy user for the first entry
from django.contrib.auth.models import User
qs=User.objects.all()
if len(qs[:])==0:
    user = User.objects.create_user(username='204269', email='lennon@jpmorgan.com', password='johnpassword')