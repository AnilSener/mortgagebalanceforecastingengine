__author__ = 'anil'
#Scriot to run distributed data transformations and forecast, simulation and variable calculations
import datetime
import math
#Pyspark based method controlling RDD transformations and executing the calculation and estimation methods
def forecastBalance(me,request,type):
    # Path for spark source folder
    try:
        from pyspark import SparkConf,SparkContext
        from pyspark.sql import Row
        conf = (SparkConf()
        .setMaster("yarn-client")
        #.setMaster("local")
        #you can shift between local and yarn-client mode, it is very important to have same python2.7 version in all servers
        .setAppName("forecastingengine")
        .set("spark.executor.memory", "512m")
        .set("spark.driver.memory", "512m")
        .set("spark.python.worker.memory", "4g") #My environment didnt support more please increase to 8gb
        .set("spark.shuffle.memoryFraction", 0.4)
        .set("spark.default.parallelism",2) #I am using only 2 nodes for execution
        .set("spark.executor.instances", 2)
        .set("spark.executor.cores", 2) #My environment didnt support more please increase to 4 cores
        )
        sc = SparkContext(conf=conf)
        #I had to configure all these resources according to my systems resources
        # otherwise I am getting errors from yarn resource manager
        # plese increase memory and cpu usage as much as you have
        # Loading portfolio text file and reshaping in fact I was going to use a parquet file but our system was very unstable so I don't want to change anything in an evolved app
        lines = sc.textFile("jpmorgan/portfolios/portfolio.tsv")
        parts = lines.map(lambda l: l.split("\t")).cache()
        #Converting Django Macro ecomonomic model objects given by user to RDD
        me_rdd=sc.parallelize(zip(me.values_list('id', flat=True),me.values_list('Date', flat=True),me.values_list('HPI_NY', flat=True),me.values_list('HPI_CA', flat=True),me.values_list('MTG', flat=True),me.values_list('userID', flat=True),me.values_list('isForecast', flat=True))).cache()
        #Taking cartesian product between two RDD, as I assumed it is much cheaper than a complex join based on date
        cart_prod=me_rdd.cartesian(parts).cache()
        #filtering macro econ relationships before the origination and convert each line to a Row by Piped RDD
        filtered_rdd=cart_prod.filter(lambda x: x[0][1] >= datetime.datetime.strptime(x[1][2],"%m/%d/%Y").date()).cache()
        #piping RDD to call fields by names in the forecast and simulations
        piped_rdd = filtered_rdd.map(lambda p: Row(F_ROW_ID=p[0][0],Date=p[0][1],HPI_NY=p[0][2],HPI_CA=p[0][3],MTG=p[0][4],userID=p[0][5],isForecast=p[0][6],ID=long(p[1][0]),Channel=p[1][1], Origination_Date=datetime.datetime.strptime(p[1][2],"%m/%d/%Y").date(), State=p[1][3],Original_Loan_Amount=long(p[1][4]), OLTV=float(p[1][5]), OFICO=int(p[1][6]), Term=int(p[1][7]), Annual_Interest_Rate=float(p[1][8]), Balance=float(p[1][9]), Age=int(p[1][10]))).cache()
        #SORTING ID (Loan id) it is a critical step necessary in the for loops because loan macro econ data date sequence
        def keyGen(val):return val[8]
        sorted_rdd = piped_rdd.sortBy(keyGen).cache()
        #partitioning the data by channel!!!
        partitioned=sorted_rdd.groupBy(lambda x: (x.Channel=='A'),numPartitions=2).cache()
        #computing all variables and expected balance via Forecast or simulation
        calculated=partitioned.mapPartitions(computeVariables) if type=="Forecast" else partitioned.mapPartitions(simulateBalanceEstimations)
        calculated.cache()
        rowList=sorted(calculated.collect())
        sc.stop() #Closing spark contexts because it is important to make it available as soon as the spark execution is completed
        #When YARN is used context is closed automatically by YARN after the task is completed
        #Repgrouping all data and aggregating the sums for forecast and simulation
        rowList=filter(lambda v: v is not None, rowList)
        from itertools import groupby
        from forecastingengine.models import MacroEcon
        for key, group in groupby(rowList, lambda item: item["Date"]):
            me_item=MacroEcon.objects.filter(userID=request.session["userID"],Date=key).get()
            me_item.estimation=sum([item["ExpectedBalance"] for item in group])
            me_item.save()
        return datetime.datetime.now()
    except ImportError as e:
        print ("Can not import Spark Modules", e)
        return None

#Forecasting & calculations execution method
def computeVariables(channel):
    for c in channel:
        originationHPI=0;previousTermEndBalance=0;amortizedBalance=0;prevCumPD=0;prevCumPP=0
        for i,l in enumerate(c[1]):
            age=(l.Date.year-l.Origination_Date.year)*12+(l.Date.month-l.Origination_Date.month)
            k=age+1;installment=0;
            if k>0:
                c=l.Annual_Interest_Rate/12;
                amortizedBalance=l.Original_Loan_Amount*((1 + c)**l.Term-(1 + c)**(k))/(((1 + c)**l.Term)-1)
                if k==1:
                    previousTermEndBalance=l.Original_Loan_Amount;
                    prevCumPD=0;prevCumPP=0
                installment=previousTermEndBalance-amortizedBalance
            HPI= l.HPI_NY if l.State == 'NY' else l.HPI_CA
            originationHPI=HPI if k==1 else originationHPI
            conditionalPP=0;conditionalPD=0;balanceEstimation=0
            LTV=calculateLTV(previousTermEndBalance,l.Original_Loan_Amount,l.OLTV,HPI,originationHPI)
            if k>0 and l.isForecast:
                conditionalPP=calculateProbPrepay(l.Annual_Interest_Rate,l.MTG,previousTermEndBalance)
                conditionalPD=calculateProbDefault(l.OFICO,LTV,age,previousTermEndBalance)
                ProbNormalPay=1-(conditionalPP+conditionalPD+prevCumPD+prevCumPP)
                ProbNormalPay=ProbNormalPay if ProbNormalPay >=0 else 0
                balanceEstimation=previousTermEndBalance-(ProbNormalPay*installment)-((1-ProbNormalPay)*previousTermEndBalance) if ProbNormalPay>0 else 0
            if k>0 and k<l.Term:
                previousTermEndBalance=amortizedBalance
                if l.isForecast:
                    prevCumPD=prevCumPD+conditionalPP;prevCumPP=prevCumPP+conditionalPD
            yield {"Date":l.Date,"ExpectedBalance":balanceEstimation}
        yield None

#Simulation execution method
import simpy as sim
from numpy import random
def simulateBalanceEstimations(channel):
    NO_OF_ITERATIONS=1000  #1000 iterations configured
    env=sim.Environment()
    resource=sim.PreemptiveResource(env, capacity=1)
    store = sim.Store(env)
    for c in channel:
        [env.process(computeSimVariables(env,random.normal(),c,store,iter_no,resource)) for iter_no in range(1,NO_OF_ITERATIONS+1,1)]
        env.run(until=1000)
        return store.items
#Simulation & calculations execution method
def computeSimVariables(env,normal,c,store,iter_no,resource):
    with resource.request(priority=iter_no) as req:
        yield req
        originationHPI=0;previousTermEndBalance=0;amortizedBalance=0;prevCumPD=0;prevCumPP=0;new_rate=0
        for i,l in enumerate(c[1]):
            age=(l.Date.year-l.Origination_Date.year)*12+(l.Date.month-l.Origination_Date.month)
            k=age+1;installment=0;
            if k>0:
                if k==1:
                    previousTermEndBalance=l.Original_Loan_Amount;
                    prevCumPD=0;prevCumPP=0
                    new_rate=l.Annual_Interest_Rate
                new_rate=calculateChangedInterestRate(new_rate,normal) if l.isForecast else l.Annual_Interest_Rate
                l.Annual_Interest_Rate=new_rate
                c=l.Annual_Interest_Rate/12;
                amortizedBalance=l.Original_Loan_Amount*((1 + c)**l.Term-(1 + c)**(k))/(((1 + c)**l.Term)-1)
                installment=previousTermEndBalance-amortizedBalance
            HPI= l.HPI_NY if l.State == 'NY' else l.HPI_CA
            originationHPI=HPI if k==1 else originationHPI
            conditionalPP=0;conditionalPD=0;balanceEstimation=0
            LTV=calculateLTV(previousTermEndBalance,l.Original_Loan_Amount,l.OLTV,HPI,originationHPI)
            if k>0 and l.isForecast:
                conditionalPP=calculateProbPrepay(l.Annual_Interest_Rate,l.MTG,previousTermEndBalance)
                conditionalPD=calculateProbDefault(l.OFICO,LTV,age,previousTermEndBalance)
                ProbNormalPay=1-(conditionalPP+conditionalPD+prevCumPD+prevCumPP)
                balanceEstimation=previousTermEndBalance-(ProbNormalPay*installment)-((1-ProbNormalPay)*previousTermEndBalance) if ProbNormalPay>0 else 0
            if k>0 and k<l.Term:
                previousTermEndBalance=amortizedBalance
                if l.isForecast:
                    prevCumPD=prevCumPD+conditionalPP;prevCumPP=prevCumPP+conditionalPD
            yield store.put({"Date":l.Date,"ExpectedBalance":balanceEstimation})
        yield env.timeout(1)
#Computing changing interest rate for the simulation
def calculateChangedInterestRate(Annual_Interest_Rate,normal):
    return Annual_Interest_Rate+(0.3*(0.06-Annual_Interest_Rate))+(0.0005*normal)
#Computing changing current LTV for the forecast and simulation
def calculateLTV(currentBalance,originationBalance,OLTV,HPI,originationHPI):
    return currentBalance/((originationBalance/OLTV)*(float(HPI)/float(originationHPI)))
#Computing changing propabability of prepayment for the forecast and simulation
def calculateProbPrepay(LoanInterestRate,MTG,currentBalance):
    return 1/(1+math.exp(3.4761-101.09*(LoanInterestRate-float(MTG))))
#Computing changing propabability of default for the forecast and simulation
def calculateProbDefault(OFICO,currentLTV,k,currentBalance):
    return 1/(1+math.exp(4.4+0.01*OFICO-4*currentLTV+0.2*min(max(k-36,0),24)+0.1*min(max(k-60,0),60)-0.05*min(max(k-120,0),120)))

