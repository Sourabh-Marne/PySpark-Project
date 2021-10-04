#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from email.parser import Parser
import re
import time
from datetime import datetime, timezone, timedelta

def date_to_dt(date):
    def to_dt(tms):
        def tz():
            return timezone(timedelta(seconds=tms.tm_gmtoff))
        return datetime(tms.tm_year, tms.tm_mon, tms.tm_mday, 
                      tms.tm_hour, tms.tm_min, tms.tm_sec, 
                      tzinfo=tz())
    return to_dt(time.strptime(date[:-6], '%a, %d %b %Y %H:%M:%S %z'))


# /!\ All functions below must return an RDD object /!\

# T1: replace pass with your code
def extract_email_network(rdd):
    import re
    def date_to_dt(date):
        def to_dt(tms):
            def tz():
                return timezone(timedelta(seconds=tms.tm_gmtoff))
            return datetime(tms.tm_year, tms.tm_mon, tms.tm_mday, 
                          tzinfo=tz())
        return to_dt(time.strptime(date[:-6], '%a, %d %b %Y %H:%M:%S %z'))
    email = list(rdd.map(lambda x: re.findall('\S+@enron.com', x)).collect()) #dot? \S+@[a-z.]*enron.com
    dt = rdd.map(lambda x: re.findall('Date.*', x))
    dt = dt.map(lambda x: str(x[0]))
    dt = dt.map(lambda x: x[6:]).collect()

    for i in range(len(dt)):
        email[i] = (email[i],dt[i])

    email_regex = r'^[A-Za-z0-9!#$%&\'*+-/=?^_`{|}~]+@[A-Za-z0-9]+(\.[a-zA-Z]+)*$'
    valid_email = lambda s: True if re.compile(email_regex).fullmatch(s) else False

    email = sc.parallelize(email)
    rdd = email.filter(lambda s: len(s[0])>1)
    rdd = rdd.map(lambda s: (s[0][0],s[0][1:],s[-1]))
    rdd = rdd.filter(lambda s: valid_email(s[0])) #check valid sender
    rdd = rdd.filter(lambda s: [valid_email(i) for i in s[1]]) #check valid recipients
    rdd = rdd.map(lambda s: [(s[0],i,date_to_dt(s[-1])) for i in s[1]]) #arranging in SRT format
    rdd = rdd.map(lambda s: [i for i in s if i[0]!=i[1]]) #check if sender and recipient is not same
    rdd = rdd.flatMap(lambda s: s).distinct() #check triples are distinct
    return rdd

# T2: replace pass with your code            
def get_monthly_contacts(rdd):
    from operator import add 
    rdd = rdd.map(lambda s: (str(s[0])+"-"+str(s[2].month)+'/'+str(s[2].year),s[1]))
    rdd = rdd.map(lambda s: (s[0],1)).reduceByKey(add)
    rdd = rdd.map(lambda s: (s[0].split("-"),s[1]))
    rdd = rdd.map(lambda s: (s[0][0],s[0][1],s[1]))
    return rdd

# T3: replace pass with your code
def convert_to_weighted_network(rdd, drange=None):
    from operator import add
    dtd = lambda x: datetime(drange[x].year, drange[x].month, drange[x].day)
    rdd = rdd.map(lambda s:(str(s[0])+"-"+str(s[1]),datetime(s[2].year,s[2].month,s[2].day)))
    if drange == None:
        rdd = rdd.map(lambda s: (s[0],1)).reduceByKey(add)
    else:
        #extracting dates
        rdd = rdd.filter(lambda s: (dtd(0) <= s[1] and s[1] <= dtd(1)))
        rdd = rdd.map(lambda s: (s[0],1)).reduceByKey(add)
        
    rdd = rdd.map(lambda s: (s[0].split("-"),s[1]))
    rdd = rdd.map(lambda s: (s[0][0],s[0][1],s[1]))
    return rdd

# T4.1: replace pass with your code
def get_out_degrees(rdd):
    from operator import add
    rdd1 = rdd.map(lambda s: (s[0],s[2])).collect()
    rdd2 = rdd.map(lambda s: (s[1],0)).collect()
    rdd1.extend(rdd2)
    rdd = sc.parallelize(rdd1)
    rdd = rdd.map(lambda s: (s[0],s[1])).reduceByKey(add)
    rdd = rdd.sortBy(lambda s: [s[1], s[0]]).collect()
    rdd.reverse()
    rdd = sc.parallelize(rdd)
    rdd = rdd.map(lambda s: (s[1],s[0]))
    return rdd

# T4.2: replace pass with your code         
def get_in_degrees(rdd):
    from operator import add
    rdd1 = rdd.map(lambda s: (s[1],s[2])).collect()
    rdd2 = rdd.map(lambda s: (s[0],0)).collect()
    rdd1.extend(rdd2)
    rdd = sc.parallelize(rdd1)
    rdd = rdd.map(lambda s: (s[0],s[1])).reduceByKey(add)
    rdd = rdd.sortBy(lambda s: [s[1], s[0]]).collect()
    rdd.reverse()
    rdd = sc.parallelize(rdd)
    rdd = rdd.map(lambda s: (s[1],s[0]))
    return rdd
