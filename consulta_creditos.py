import sqlalchemy
from sqlalchemy.sql import text
from utils import to_json,post,CustomJsonEncoder,get
import concurrent.futures
import requests
import functools
import json
import datetime
from concurrent.futures import as_completed



def task(endpoint,header,request_timeout,credito_id):
   
    output=[]
    response={}


    json_string="{}"
    response['object_key']=int(credito_id)
    response['original_request']={}
    response['end_point']=endpoint
    endpoint=endpoint +str(credito_id) + '/'
    
    try:
        #print(json_string)
        r=get(json_string,end_point=endpoint,header=header)
        #print(r.text)
        #print(r.reason)
        #print(r.ok)
        #print(r.status_code)


        response['text']=r.text
        response['status_code']=r.status_code
        response['reason']=r.reason
        response['message']=r.text
        response['ok']=r.ok
        response['elapsed']=str(r.elapsed)
     
        output.append(response)

    except requests.exceptions.ReadTimeout as e:
        print(e)
        response['status_code']=504
        response['reason']='Timeout Error'
        response['text']=f'Se alcanzó el timeout despues de {request_timeout} segundos.'
        response['message']=str(e)
        
        response['ok']=False
        response['elapsed']=None
        output.append(response)
        return response
    

    except Exception as e:
        print(e)
        response['status_code']=500
        response['reason']="Task Exception"
        response['text']="Ocurrio un error al procesar la tarea ."
        response['message']= str(e)
        response['ok']=False
        response['elapsed']=None
        output.append(response)
        return response
    

    return response



def main():
    start_job = datetime.datetime.now()
    NUM_THREADS=8
    ENDPOINT='http://10.90.100.70:3000/safi/api/creditodetalle/'
    HEADERS={'Content-type': 'application/json','Autorizacion':'RUZJU1lTMjpWb3N0cm8xMzEw','User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'}
    
    TIMEOUT=30
    EXIGIBLE_PARCIAL="select CreditoID from CREDITOS  where Estatus in ('V','B') limit 100;" 

    engine=sqlalchemy.create_engine('mysql+pymysql://root:Pr0gr3S4Sql_%1@10.90.100.70:3306/microfin')
    sql=text(EXIGIBLE_PARCIAL)
    request_task=functools.partial(task,ENDPOINT,HEADERS,TIMEOUT)
    request_results=[]
    with engine.connect() as db:
        resultados=db.execute(sql)
        lista_creditos=resultados.mappings().all()
      
    print(lista_creditos)
 

    with concurrent.futures.ThreadPoolExecutor(NUM_THREADS) as executor:
        futures = [executor.submit(request_task, row['CreditoID']) for row in lista_creditos]
        #request_results=as_completed(futures)
        for future in as_completed(futures):
            request_results.append(future.result())
    error=0
    success=0
    for res in request_results:
                         
        print(f'Procesado CreditoID :{ res["object_key"] }')     
        if(not res["ok"]):
            error=error+1
            print(f'Petición rechazada: [ Endpoint response: {res["status_code"]} -{res["reason"]}  -  elapsed at: { res["elapsed"] }]')
            print(f'        Respuesta: {res["status_code"]}')
            print(f'        Motivo: {res["reason"]}')
            print(f'        {res["text"]}')
            print(f'=>>\n { res["original_request"]} \n<<=')
                
        else:
            # para  safi
            response=json.loads(res["text"])
            if(response['responseCode'] != '000000'):
                error=error+1
                print(f'Petición rechazada: [ Endpoint response: {response["responseCode"]} -{response["responseMessage"]}  -  elapsed at: { res["elapsed"] }]')
                print(f'=>>\n { res["original_request"]} \n<<=')
            else:
                success=success+1
                print(f'Petición exitosa : [ Endpoint response: {res["status_code"]} -{response["responseMessage"]}  - elapsed at: { res["elapsed"] } ]')
                    
    end_job = datetime.datetime.now()
    elapsed_time=end_job-start_job
    print(f'☑ Procesados ' + str( 100 ) +' registros,   Exitosos:'  + str(success) + ', Fallidos:' + str(error)+ ', en ' +  str(elapsed_time.total_seconds())  + ' segundos.')
main()