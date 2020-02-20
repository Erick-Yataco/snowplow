from __future__ import print_function
import base64
import json
from pygeoip import GeoIP
import boto3
gi = GeoIP('GeoLiteCity.dat')


def check_ip_address(ip):
    a = ip.split('.')
    if len(a) != 4:
        return False
    for i in range(0, 4):
        if not a[i].isdigit():
            return False
        j = int(a[i])
        if j < 0 or j > 255:
            return False
    return True


def get_location(ip):
    gir = gi.record_by_addr(ip)
    if gir is None:
        return None
    if 'city' in gir:
        city = gir['city']
        print(city)
        return city
    return None


def lambda_handler(event, context):
    output = []

    for record in event['records']:

        payload = base64.b64decode(record['data']).decode("utf-8", "ignore")

        if(payload.find('data') > 0):
            startIndex = payload.find('"data":')
            temp1 = payload[startIndex+7:]
            endIndex = temp1.find(']}')
            temp2 = temp1[:endIndex+1]
            try:
                json_object = json.loads(temp2)
                for i in range(0, len(json_object)):
                    if(payload.find('X-Forwarded-For:') > 0):
                        ip_start_index = payload.find('X-Forwarded-For:')
                        if(payload.find('X-Forwarded-Proto:') > 0):
                            ip_end_index = payload.find('X-Forwarded-Proto:')
                            ip = payload[ip_start_index+17:ip_end_index]
                            print(ip)
                            if (check_ip_address(ip[:-4:])):
                                city = get_location(ip[:-4:])
                                json_object[i]['city'] = city
                str_object = json.dumps(json_object)
                temp3 = str_object[1:-1]
                temp3 = temp3+"\n"
                temp4 = temp3.replace("},{", "}\n{")
                temp5 = temp4.replace("}, {", "}\n{")
                outputPayload = json.loads(json.dumps(temp5))
                print(outputPayload)
            except ValueError as e:
                continue
        else:
            continue
        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode(outputPayload)
        }

        output.append(output_record)

    print('Successfully processed {} records.'.format(len(event['records'])))

    return {'records': output}
