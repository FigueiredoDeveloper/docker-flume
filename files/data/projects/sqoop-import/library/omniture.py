# -*- coding: utf-8 -*-
from datetime import date, timedelta, datetime
from collections import defaultdict
import urllib2, time, binascii, sha, json

class Omniture:
    
    YESTERDAY_DATE = (date.today() - timedelta(1)).strftime("%Y-%m-%d")
    
    def __init__(self, user_name, shared_secret):
        self.user_name = user_name
        self.shared_secret = shared_secret
    
    def translate_status(self,status):
        if status == 'queued':
            status = 'agendado'
        if status == 'failed':
            status = 'falha'
        if status == 'ready':
            status = 'criado'
        if status == 'done':
            status = 'pronto'
        return status

    def __get_header(self):
        nonce = str(time.time())
        base64nonce = binascii.b2a_base64(binascii.a2b_qp(nonce))
        created_date = time.strftime("%Y-%m-%dT%H:%M:%SZ",  time.gmtime())
        sha_object = sha.new(nonce + created_date + self.shared_secret)
        password_64 = binascii.b2a_base64(sha_object.digest())
        s = 'UsernameToken Username="%s", PasswordDigest="%s", Nonce="%s", Created="%s"'
        return s % (self.user_name, password_64.strip(),
                    base64nonce.strip(),
                    created_date)

    def __get_header_api_1_4(self):
        nonce = str(time.time())
        base64nonce = binascii.b2a_base64(binascii.a2b_qp(nonce))
        created_date = time.strftime("%Y-%m-%dT%H:%M:%SZ",  time.gmtime())
        sha_object = sha.new(nonce + created_date + self.shared_secret)
        password_64 = binascii.b2a_base64(sha_object.digest())
        s = 'UsernameToken Username="%s", PasswordDigest="%s", Nonce="%s", Created="%s"'
        return s % (self.user_name, password_64.strip(),
                    base64nonce.strip(), 
                    created_date)

    def request(self, method, request_data):
        """Send a request to the Omniture REST API
        Parameters:
        method-- The Omniture Method Name (ex. Report.QueueTrended, Company.GetReportSuites)
        request_data-- Details of method invocation, in Python dictionary/list form.
        """
        api_url = 'https://api.omniture.com/admin/1.3/rest/?method=%s'
        request = urllib2.Request(api_url % method, json.dumps(request_data))
        request.add_header('X-WSSE', self.__get_header())
        return json.loads(urllib2.urlopen(request).read())


    def requestQueue(self, method, request_data,max_polls=120,max_retries=3):
        """Send a report request to the Omniture REST API, and wait for its response.
         Omniture is polled every 10 seconds to determine if the response is ready.
        When the response is ready, it is returned.
        Parameters:
        method-- The Omniture Method Name (ex. Report.QueueTrended, Report.QueueOvertime)

        request_data-- Details of method invocation, in Python dictionary/list form.

        max_polls-- The max number of times that Omniture will be polled to
                    see if the report is ready before failing out.

        max_retries-- The max number of times that we allow Omniture to report
                a failure, and retry this request before throwing an Exception.
        """
        status, status_resp = "", ""
        num_retries=0

        while status != 'done' and num_retries < max_retries:
            print method
            print request_data
            status_resp = self.request(method, request_data)
            # Check if is a realtime report and return immediately
            if method == "Report.GetRealTimeReport":
                return status_resp
            report_id = status_resp['reportID']
            status = status_resp['status']

            print "Status para o relatório ID %s é %s" % (report_id, self.translate_status(status))
            polls=0
            while status != 'done' and status != 'failed':
                if polls > max_polls:
                    raise Exception("Error:  Excedeu o tempo de espera\
                     e de tentativa para o relatório ID %s" % report_id)
                time.sleep(10)
                status_resp = self.request('Report.GetStatus', {"reportID" : report_id})
                status = status_resp['status']
                print "Status para o relatório ID %s é %s" % (report_id, self.translate_status(status))

            if status == 'failed':
                num_retries += 1
                print "Falha ao tentar gerar o relatório do Adobe Analytics, Tentando novamente o mesmo relatório."

        # We exit the while loop only when the report is done or the report
        # has failed and passed the max retries.
        if status == 'failed':
            raise Exception("Error: Falha ao tentar gerar o relatório do Adobe Analytics excedeu o número de tentativas: %s \
            A resposta final é %s" % (max_retries, status_resp))

        #We are all good, return the report
        return self.request('Report.GetReport', {'reportID' : report_id})

    def request_api_1_4(self, method, request_data):
        """Send a request to the Omniture REST API
        Parameters:
        method-- The Omniture Method Name (ex. Report.QueueTrended, Company.GetReportSuites)
        request_data-- Details of method invocation, in Python dictionary/list form.
        """
        try:
            api_url = 'https://api.omniture.com/admin/1.4/rest/?method=%s'
            request = urllib2.Request(api_url % method)
            request.add_data(json.dumps(request_data))
            request.add_header('X-WSSE', self.__get_header())
            resposta = json.loads(urllib2.urlopen(request).read())
        except Exception, error:
            resposta = {"error": str(error)}

        return resposta

    def requestQueue_1_4(self, method, request_data,max_polls=120,max_retries=3):
        """Send a report request to the Omniture REST API, and wait for its response.
         Omniture is polled every 10 seconds to determine if the response is ready.
        When the response is ready, it is returned.
        Parameters:
        method-- The Omniture Method Name (ex. Report.QueueTrended, Report.QueueOvertime)

        request_data-- Details of method invocation, in Python dictionary/list form.

        max_polls-- The max number of times that Omniture will be polled to
                    see if the report is ready before failing out.

        max_retries-- The max number of times that we allow Omniture to report
                a failure, and retry this request before throwing an Exception.
        """
        status, status_resp = "queued", ""
        num_retries=0
        while status != 'done' and num_retries < max_retries:
            print method
            print request_data
            status_resp = self.request_api_1_4(method, request_data)

            if 'error' in status_resp.keys():
                raise Exception("Error request adobe api: " + status_resp["error"])

            report_id = status_resp['reportID']

            print "Status para o relatório ID %s é %s" % (report_id, self.translate_status(status))
            polls=0
            while status != 'done' and status != 'failed':
                if polls > max_polls:
                    raise Exception("Error:  Excedeu o tempo de espera\
                     e de tentativa para o relatório ID %s" % report_id)

                time.sleep(20)

                status_resp = self.request_api_1_4('Report.Get', {"reportID": report_id})

                if 'error' in status_resp.keys():
                    status = 'queued'
                else:
                    status = 'done'

                print "Status para o relatório ID %s é %s" % (report_id, self.translate_status(status))

            if status == 'failed':
                num_retries += 1
                print "Falha ao tentar gerar o relatório do Adobe Analytics, Tentando novamente o mesmo relatório."

        # We exit the while loop only when the report is done or the report
        # has failed and passed the max retries.
        if status == 'failed':
            raise Exception("Error: Falha ao tentar gerar o relatório do Adobe Analytics excedeu o número de tentativas: %s \
            A resposta final é %s" % (max_retries, status_resp))

        #We are all good, return the report
        #return self.request_api_1_4('Report.GetReport', {'reportID': report_id})
        return status_resp

    def get_count_from_report(self,
                            report_suite_id, 
                            metric, 
                            element=None, 
                            selected_element_list=None, 
                            date_from=YESTERDAY_DATE, 
                            date_to=YESTERDAY_DATE, 
                            date_granularity="day", 
                            return_one_total_result = True):
        """Send a report request to the Omniture REST API, and return the total 
           count from its response for all selected elements (if any).
        Parameters:
        report_suite_id-- The name of the report suite configured in Omniture
        
        metric-- See Omniture documentation for full list - 
                 examples include visits, pageViews, visitorsMonthly
                 
        element-- Optional.  If ommitted, the only element used is the date, 
              and the report is run an Overtime report.  
              See Omniture docs for full list - examples include page, prop11
               
        selected_element_list-- Optional.  Python list of all element values 
                        to filter on, for example ["Homepage", "Media Guide"]
                        
        date_from-- Optional.  If ommitted, assumed to be yesterday's date.  
                    To set, use date in string form YYYY-MM-DD.
                    
        date_to-- Optional.  If ommitted, assumed to be yesterday's date.  
                  To set, use date in string form YYYY-MM-DD.
                  
        date_granularity--Optional.  If ommitted, assumed to be "day"
        
        return_one_total_result --Optional.  If ommitted, assumed to be "True", 
            and a single integer is returned, which is the sum of the metric for
            the entire date range.  If false, then a dictionary of results is 
            returned, with one result per dat in the date range entered.
        """
        metrics = [{"id":metric}]
        
        # Determine which type of report to run based on whether there is an 
        # element needed besides date/time
        if element == None:
            request_type = 'Report.QueueOvertime'
            elements = None
        else:
            request_type= 'Report.QueueTrended'
            elements = [{"id":element, "selected": selected_element_list }]
        
        #Invoke Omniture API with properly formed JSON Request
        response = self.requestQueue(request_type,
                                        {"reportDescription":  
                                            {"reportSuiteID" :report_suite_id,
                                             "dateFrom":date_from,
                                             "dateTo":date_to,
                                             "dateGranularity":date_granularity,
                                             "metrics": metrics, 
                                             "elements" : elements
                                            }})
        if response["status"] != "done":
            raise Exception("Error:  Resposta final é %s" % response)
        
        report = response["report"]
        
        if return_one_total_result:
            if selected_element_list == None:
                # Using the first element here since we only support one metric.  
                # If we want to support more than one metric, would need to handle that here.
                try:
                    return int(report["totals"][0])
                except ValueError:
                    return float(report["totals"][0])
            total_for_selected_elements = 0
            for datum in report["data"]:
                if datum["name"] in selected_element_list:
                    # Using the first element here since we only support one metric.
                    # If we want to support more than one metric, would need to handle that here.
                    total_for_selected_elements += int(datum["counts"][0])                   
            return total_for_selected_elements
        
        #Handle returning a dictionary of results, with one entry per day.         
        else:
            # Using a defaultdict here to allow 0 to be set for every value the 
            # first time through.  Makes code cleaner for QueueTrended, as we 
            # dont need to check the presence of the key first.
            result_dict = defaultdict(int)
            for datum in report["data"]:
                if request_type == "Report.QueueOvertime":
                    result_dict[datum["name"]] = datum["counts"][0]
                elif request_type == "Report.QueueTrended":
                    if selected_element_list == None or datum["name"] in selected_element_list:
                        for day_breakdown in datum["breakdown"]:
                            result_dict[day_breakdown["name"]] += int(day_breakdown["counts"][0])
            return result_dict
            

    def query_report(self, request_type, query, dateFrom=None, dateTo=None):
        jsonQuery = json.loads(query, "utf-8")
        if not dateFrom:
            jsonQuery['reportDescription']['dateFrom'] = str(datetime.now().date() - timedelta(1))
        else:
            jsonQuery['reportDescription']['dateFrom'] = dateFrom
        if not dateTo:
            jsonQuery['reportDescription']['dateTo'] = str(datetime.now().date())
        else:
            jsonQuery['reportDescription']['dateTo'] = dateTo

        response = self.requestQueue(request_type, jsonQuery)
        report = response["report"]
        return report

    def query_report_1_4(self, request_type, query, dateFrom=None, dateTo=None):
        jsonQuery = json.loads(query, "utf-8")
        if not dateFrom:
            jsonQuery['reportDescription']['dateFrom'] = str(datetime.now().date() - timedelta(1))
        else:
            jsonQuery['reportDescription']['dateFrom'] = dateFrom
        if not dateTo:
            jsonQuery['reportDescription']['dateTo'] = str(datetime.now().date())
        else:
            jsonQuery['reportDescription']['dateTo'] = dateTo

        response = self.requestQueue_1_4('Report.Queue', jsonQuery)
        report = response["report"]
        return report

    def ranked_report(self,
                    report_suite_id, 
                    metric, 
                    element,
                    date_from=YESTERDAY_DATE,
                    date_to=YESTERDAY_DATE):
        """ 
        Run a Report.QueueRanked and return a list with the data in dict format.
        Regard date_from and date_to, all others parameters are required!!
        Parameters:
        report_suite_id-- The name of the report suite configured in Omniture
        
        metric-- See Omniture documentation for full list - 
                 examples include visits, pageViews, visitorsMonthly
                 
        element-- See Omniture docs for full list - examples page, prop11
                You can pass a list with more than one element this way:
                [{"id": "product"}, {"id": "evar10"}]
        """
        
        metrics = [{"id": metric}]
        
        # Check just one element was passed with means is not a list
        if not isinstance(element, list):
            elements = [{"id": element}]
        else:
            elements = element
        
        response = self.requestQueue('Report.QueueRanked',
                                        {"reportDescription":  
                                            {"reportSuiteID" :report_suite_id,
                                             "dateFrom":date_from,
                                             "dateTo":date_to,
                                             "metrics": metrics, 
                                             "elements": elements
                                            }})
                                            
        # Return only data
        return response['report']['data']
            
    def get_metrics(self, report_suite_id, report_type):
        try:
           request_data = {
                "existingElements":[],
                "existingMetrics":[],
                "reportSuiteID": "b2w-acom",
                "reportType":"ranked"
            }
           response = self.request_api_1_4('Report.GetMetrics', request_data)
           print response.read()
           # for item in response:
           #     print("\n id" + item['id'] + "\n Name:" + item['name'])
        except BaseException, e:
            print(e.read())
            print "Error while trying to get metrics for report id %s" % report_suite_id
        pass

    def real_time_report(self,
                        report_suite_id, 
                        metric, 
                        element=None,
                        period_count=3,
                        period_minutes=1,
                        algorithm= "most popular",
                        period_offset=0,
                        algorithm_argument = None,
                        first_rank_period = None,
                        floor_sensitivity = None):
        """
        Run a Report.GetRealTimeReport and return the data in a list
        Parameters:
        report_suite_id-- The name of the report suite configured in Omniture
            
        metric-- See Omniture documentation for full list - 
                 examples include visits, pageViews, visitorsMonthly
                 
        element-- See Omniture docs for full list - examples page, prop11
                You can pass a list with more than one element this way:
                [{"id": "product"}, {"id": "evar10"}]
            
        period_count -- The number of periods of data to return
        
        period_minutes -- Number of minutes for one period
        
        period_offset --Number of minutes before the current minute to run the report.
        
        algorithm_argument -- Specifies how to order the values for 
                             Most Popular, Gainers or Losers.
                             
        first_rank_period -- Computes the ranking of elements by considering 
            the element's counts from the firstRankPeriod to the final period.
            
        floor_sensitivity -- A factor between 0 and 1 that is used to cut off 
                            low-count items from percentage ranking. 
                   
        """
            
        response = self.requestQueue('Report.GetRealTimeReport',
                                        {"reportDescription":  
                                            {"reportSuiteID": report_suite_id,
                                             "periodCount": period_count,
                                             "periodMinutes": period_minutes,
                                             "periodOffset": period_offset,
                                             "algorithm": algorithm,
                                             "algorithmArgument": algorithm_argument,
                                             "firstRankPeriod": first_rank_period,
                                             "floorSensitivity": floor_sensitivity,
                                             "metrics": metric, 
                                             "elements" : element
                                            }})
                                            
                                            
        return response['report']['data']
