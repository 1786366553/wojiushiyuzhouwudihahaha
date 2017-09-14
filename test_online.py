# -*- coding: UTF-8 -*-
import DNS
import urllib
import socket
import MySQLdb
import pymongo
import threading
from time import ctime,sleep


def domain_online_query(query, type):
    if type == "A":
        return dict_build(query, type)
    elif type == "NS":
        return dict_build(query, type)
    elif type == "CNAME":
        return dict_build(query, type)
    elif type == "SOA":
        return dict_build(query, type)
    elif type == "PTR":
        return dict_build(query, type)
    elif type == "CNAME":
        return dict_build(query, type)
    elif type == "CNAME":
        return dict_build(query, type)
    elif type == "STATUS":
        return domain_online_judge(query)
    elif type == "ANY":
        return any_dict_combine(query)


def dict_build(query, type):
    dict_domain = {}
    dict_domain['query_domain'] = query
    dict_domain['query_type'] = type
    dict_domain['query_answer'] = record_combine(query, type)
    return dict_domain


def any_dict_combine(query):
    dict_domain = {}
    dict_domain['query_domain'] = query
    dict_domain['query_type'] = "all"
    dict_domain['A_record'] = record_combine(query, "A")
    dict_domain['NS_record'] = record_combine(query, "NS")
    dict_domain['CNAME_record'] = record_combine(query, "CNAME")
    dict_domain['SOA_record'] = record_combine(query, "SOA")
    dict_domain['PTR_record'] = record_combine(query, "PTR")
    dict_domain['MX_record'] = record_combine(query, "MX")
    dict_domain['TXT_record'] = record_combine(query, "TXT")
    dict_domain_total = dict_domain.copy()
    dict_domain_total.update(domain_online_judge(query))
    return dict_domain_total


def record_combine(query, type):
    dict_a_record = {}
    try:
        if type == "A":
            type_query = DNS.Type.A
        elif type == "NS":
            type_query = DNS.Type.NS
        elif type == "CNAME":
            type_query = DNS.Type.CNAME
        elif type == "SOA":
            type_query = DNS.Type.SOA
        elif type == "PTR":
            type_query = DNS.Type.PTR
        elif type == "MX":
            type_query = DNS.Type.MX
        elif type == "TXT":
            type_query = DNS.Type.TXT
        DNS.DiscoverNameServers()
        reqobj = DNS.Request()
        answerobj_a = reqobj.req(
            name=query, qtype=type_query, server="222.194.15.253")
        if not len(answerobj_a.answers):
            dict_a_record = {type: 'not found'}
        else:
            for item in answerobj_a.answers:
                if item['typename'] == "SOA":
                    dict_a_record[item['typename']] = soa_tuple_operate(item['data'])
                else:
                    try:
                        if dict_a_record[item['typename']]:
                            dict_a_record[item['typename']] = dict_a_record[item['typename']] + " " + item['data']
                    except:
                        dict_a_record[item['typename']] = item['data']
    except:
        dict_a_record = {type: 'timeout'}
    return dict_a_record


def soa_tuple_operate(tuple_soa):
    soa_dict = {}
    soa_dict['name_server'] = tuple_soa[0]
    soa_dict['responsible_person'] = tuple_soa[1]
    soa_dict['serial'] = tuple_soa[2][1]
    soa_dict['refresh'] = {'second':tuple_soa[3][1],'time':tuple_soa[3][2]}
    soa_dict['retry'] = {'second':tuple_soa[4][1],'time':tuple_soa[4][2]}
    soa_dict['expire'] = {'second':tuple_soa[5][1],'time':tuple_soa[5][2]}
    soa_dict['minimum'] = {'second':tuple_soa[6][1],'time':tuple_soa[6][2]}
    return soa_dict


def record_judge(query):
    try:
        DNS.DiscoverNameServers()
        reqobj = DNS.Request()
        answerobj_a = reqobj.req(
            name=query, qtype=DNS.Type.A, server="222.194.15.253")
        if len(answerobj_a.answers):
            return 1
        else:
            pass
    except:
        pass
    try:
        DNS.DiscoverNameServers()
        reqobj = DNS.Request()
        answerobj_a = reqobj.req(
            name=query, qtype=DNS.Type.MX, server="222.194.15.253")
        if len(answerobj_a.answers):
            return 1
        else:
            pass
    except:
        pass
    return 0


def http_code(query):
    '''
    查询http状态码
    '''
    try:
        status = urllib.urlopen(query)
        return str(status.getcode())
    except:
        return "error"


def domain_online_judge(query):
    domain = "http://" + query + "/"
    if(record_judge(query) == 1 and (http_code(domain)[0] == "2" or http_code(domain)[0] == "3")):
        dict_domain = {}
        dict_domain['query_domain'] = query
        dict_domain['status'] = "online"
        dict_domain['http_code'] = http_code(domain)
        return dict_domain
    else:
        dict_domain = {}
        dict_domain['query_domain'] = query
        dict_domain['status'] = "not online"
        dict_domain['http_code'] = http_code(domain)
        return dict_domain


def mongo_1():
    count = 0
    connection=pymongo.MongoClient('172.29.152.152',27017)
    db=connection.domain_cdn_analysis
    collection=db.domain_cdn
    for data in collection.find({'flag':0,'id_count':{'$gt':0,'$lt':250000}}):
        count = count + 1
        domain = str(data['domain'])
        domain_2 = "www." + domain
        a_record = domain_online_query(domain_2[:-1],"A")['query_answer']
        cname_record = domain_online_query(domain_2[:-1],"CNAME")['query_answer']
        ns_record = domain_online_query(domain_2[:-1],"NS")['query_answer']
        collection.update_many({'_id':data['_id']},{'$set':{'A_record':a_record,'CNAME_record':cname_record,'NS_record':ns_record,'flag':1}})
        if count == 100:
            print ctime() + "time1"
            count = 0

def mongo_2():
    count = 0
    connection=pymongo.MongoClient('172.29.152.152',27017)
    db=connection.domain_cdn_analysis
    collection=db.domain_cdn
    for data in collection.find({'flag':0,'id_count':{'$gt':249999,'$lt':500000}}):
        count = count + 1
        domain = str(data['domain'])
        domain_2 = "www." + domain
        a_record = domain_online_query(domain_2[:-1],"A")['query_answer']
        cname_record = domain_online_query(domain_2[:-1],"CNAME")['query_answer']
        ns_record = domain_online_query(domain_2[:-1],"NS")['query_answer']
        collection.update_many({'_id':data['_id']},{'$set':{'A_record':a_record,'CNAME_record':cname_record,'NS_record':ns_record,'flag':1}})
        if count == 100:
            print ctime() + "time2"
            count = 0


def mongo_3():
    count = 0
    connection=pymongo.MongoClient('172.29.152.152',27017)
    db=connection.domain_cdn_analysis
    collection=db.domain_cdn
    for data in collection.find({'flag':0,'id_count':{'$gt':499999,'$lt':750000}}):
        count = count + 1
        domain = str(data['domain'])
        domain_2 = "www." + domain
        a_record = domain_online_query(domain_2[:-1],"A")['query_answer']
        cname_record = domain_online_query(domain_2[:-1],"CNAME")['query_answer']
        ns_record = domain_online_query(domain_2[:-1],"NS")['query_answer']
        collection.update_many({'_id':data['_id']},{'$set':{'A_record':a_record,'CNAME_record':cname_record,'NS_record':ns_record,'flag':1}})
        if count == 100:
            print ctime() + "time3"
            count = 0


def mongo_4():
    count = 0
    connection=pymongo.MongoClient('172.29.152.152',27017)
    db=connection.domain_cdn_analysis
    collection=db.domain_cdn
    for data in collection.find({'flag':0,'id_count':{'$gt':749999,'$lt':1000000}}):
        count = count + 1
        domain = str(data['domain'])
        domain_2 = "www." + domain
        a_record = domain_online_query(domain_2[:-1],"A")['query_answer']
        cname_record = domain_online_query(domain_2[:-1],"CNAME")['query_answer']
        ns_record = domain_online_query(domain_2[:-1],"NS")['query_answer']
        collection.update_many({'_id':data['_id']},{'$set':{'A_record':a_record,'CNAME_record':cname_record,'NS_record':ns_record,'flag':1}})
        if count == 100:
            print ctime() + "time4"
            count = 0

threads = []
t1 = threading.Thread(target=mongo_1)
threads.append(t1)
t2 = threading.Thread(target=mongo_2)
threads.append(t2)
t3 = threading.Thread(target=mongo_3)
threads.append(t3)
t4 = threading.Thread(target=mongo_4)
threads.append(t4)



if __name__ == "__main__":
    print ctime() + "starttime"
    for t in threads:
        t.setDaemon(True)
        t.start()
    for t in threads:
        t.join()





















