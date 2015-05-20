#USER INTERFACE VIEW CODE FOR DJANGO FRAMEWORK
from django.template import RequestContext
from django.shortcuts import render_to_response
from forecastingengine.models import MacroEcon,Forecast,ForecastResultsTable
from datetime import datetime
from .forms import UploadFileForm
from django.contrib.auth import authenticate
from django_tables2 import RequestConfig
def calculator_view(request):
    response_dict={"response_msg":""};target='index.html'
    #generating a dummy userID for session since each user will have to work concurrently their inputs should be independent from each other
    if request.user.is_authenticated():
        authenticate(username='204269',password='johnpassword')
        request.session["userID"]=int(request.user.username)
    form = UploadFileForm()
    response_dict['form']=form
    if request.session["userID"]!=None:
        me=MacroEcon.objects.filter(userID=request.session["userID"],isForecast=True).all()
        if me!=None:
            me=me.extra(order_by = ['-id'])
            table = ForecastResultsTable(me)
            response_dict["table"]=table
            RequestConfig(request).configure(table)
            if request.method == 'POST':
                form = UploadFileForm(request.POST, request.FILES)
                if form.is_valid():
                    result=storeUploadedFile(request.FILES['file'],request.session["userID"],datetime.strptime(request.POST["startdate"],"%m/%d/%Y"))
                    if result:
                        me=MacroEcon.objects.filter(userID=request.session["userID"],isForecast=True).all().extra(order_by = ['-id'])
                        table = ForecastResultsTable(me)
                        RequestConfig(request).configure(table)
                        response_dict["table"]=table
                        response_dict["response_msg"]+="All macro economical inputs are added to DB. You can now Forecast or Simulate the balance estimations."
                    else:
                        response_dict["response_msg"]+="Macro economical inputs cannot be added to DB."
                    return render_to_response(target,response_dict,context_instance=RequestContext(request))
                else:
                    response_dict["response_msg"]+="Please provide a date and a .tsv file and press 'Add' button to give new macro economical input.Then press 'Forecast' button to forecast the portfolio balance. "
                    return render_to_response(target,response_dict,context_instance=RequestContext(request))
            if request.method == 'GET' and len(request.GET)>0:
                if "Forecast" in request.GET or "Simulate" in request.GET:
                    me=MacroEcon.objects.filter(userID=request.session["userID"]).all()
                    if len(me[:])>0:
                        f=Forecast(me,request,"Forecast") if "Forecast" in request.GET else Forecast(me,request,"Simulate") if "Simulate" in request.GET else None
                        if f.status:
                            me=MacroEcon.objects.filter(userID=request.session["userID"],isForecast=True).all().extra(order_by = ['-id'])
                            table = ForecastResultsTable(me)
                            RequestConfig(request).configure(table)
                            response_dict["table"]=table
                            response_dict["response_msg"]+="Forecast Completed." if "Forecast" in request.GET else "Simulation Completed."
                        else:
                            response_dict["response_msg"]+="Please provide a date and a .tsv file and press 'Add' button to give new macro economical input.Then press 'Forecast' button to forecast the portfolio balance. "
                        return render_to_response(target,response_dict,context_instance=RequestContext(request))
                    else:
                        response_dict["response_msg"]+="Please provide a date and a .tsv file and press 'Add' button to give new macro economical input.Then press 'Forecast' button to forecast the portfolio balance. "
                        return render_to_response(target,response_dict,context_instance=RequestContext(request))
                else:
                    response_dict["response_msg"]+="Please provide a date and a .tsv file and press 'Add' button to give new macro economical input.Then press 'Forecast' button to forecast the portfolio balance. "
                    return render_to_response(target,response_dict,context_instance=RequestContext(request))
            else:
                response_dict["response_msg"]+="Please provide a date and a .tsv file and press 'Add' button to give new macro economical input.Then press 'Forecast' button to forecast the portfolio balance. "
                return render_to_response(target,response_dict,context_instance=RequestContext(request))

#The script below loads all historical and forecasted macro-economic data from a file to SQL lite DB
def storeUploadedFile(f,userID,startDate):
    try:
        objs=MacroEcon.objects.filter(userID=userID)
        if len(objs.all()[:])>0:
            objs.delete()
        dest_file='static-assets/sampledata/macro_econ'+'_'+str(userID)+'_'+str(datetime.now())+'.tsv'
        with open(dest_file, 'wb+') as destination:
            for chunk in f.chunks():
                destination.write(chunk)
        with open(dest_file, 'r') as f:
            for i,line in enumerate(f.readlines()):
                if i>0:
                    splittedline = line.strip().split(' ')
                    me=MacroEcon()
                    me.Date=datetime.strptime(splittedline[0],"%m/%d/%Y")
                    me.HPI_NY=float(splittedline[1])
                    me.HPI_CA=float(splittedline[2])
                    me.MTG=float(splittedline[3])
                    me.userID=userID
                    me.isForecast=me.Date>=startDate
                    me.save()
        return True
    except Exception:
        print Exception.message
        return False

