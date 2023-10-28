import sqlalchemy
from sqlalchemy.sql import text
from utils import to_json,post,CustomJsonEncoder
import concurrent.futures
import requests
import functools
import json
from concurrent.futures import as_completed
import datetime




def task(endpoint,header,request_timeout,json_request):
   
    output=[]
    response={}
    credito_id=json_request['key']
    json_string=json_request['string']
    response['original_request']=json_string
    response['object_key']=int(credito_id)
    response['end_point']=endpoint

    #print(type(json_string))
    #print(type(payload))

    #json_string = dict(json_string)

    #rint(type(json_string))

  

    
    try:
        #print(json_string)
        r=post(json_string,end_point=endpoint,header=header)
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
    ENDPOINT='http://10.90.100.70:2000/microfinws/credit/creditPayment'
    HEADERS={'Content-type': 'application/json','Autorizacion':'dXN1YXJpb1BydWViYVdTOjEyMw==','User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'}
    
    TIMEOUT=30
    EXIGIBLE_PARCIAL="select cast(ClienteID as char) as customerNumber, 'E' as paymentType, cast(CreditoID as char) as creditNumber, cast(CuentaID as char) as savingsAccountNumber , 100  as amount ,1 as cashNumber,201 as deviceID,2 as branchOffice,'999' as keyPromoter,'1' as mobileFolio from CREDITOS   where Sucursal=2  and Estatus='B' limit 2;" 

    engine=sqlalchemy.create_engine('mysql+pymysql://root:Pr0gr3S4Sql_%1@10.90.100.70:3306/microfin')
    sql=text(EXIGIBLE_PARCIAL)
    request_task=functools.partial(task,ENDPOINT,HEADERS,TIMEOUT)
    request_results=[]
    with engine.connect() as db:
        resultados=db.execute(sql)
        results=resultados.mappings().all()
        #print(type(results[0]))
      
    #json_string = json.dumps(results,cls=CustomJsonEncoder)

    #print(json_string)
        #r=resultados.mappings().all()
        #resultados.mappings().all()
        #print(type(resultados[0]))
        json_requests=to_json(results,'creditNumber')
        #print(json_requests)

    with concurrent.futures.ThreadPoolExecutor(NUM_THREADS) as executor:
        futures = [executor.submit(request_task, json) for json in json_requests]
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
                print(f'Petición exitosa : [ Endpoint response: {res["status_code"]} -{response["responseMessage"]}  - Trans: {response["folio"]} - elapsed at: { res["elapsed"] } ]')

    end_job = datetime.datetime.now()
    elapsed_time=end_job-start_job
    print(f'☑ Procesados ' + str( 100 ) +' registros,   Exitosos:'  + str(success) + ', Fallidos:' + str(error)+ ', en ' +  str(elapsed_time.total_seconds())  + ' segundos.')                    

main()