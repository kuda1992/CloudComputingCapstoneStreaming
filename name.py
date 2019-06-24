import json
import boto3



s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

tableNames = ['top-airports-for-each-airport', 'best-flight-on-a-given-date', 'average-airport-delay-for-each-airport', 'top-carriers-for-each-airport']


def record_creator_factory(key, cols):
    if key == tableNames[0]:
        return top_airports_for_each_airport_record_creator(cols)
    if key == tableNames[1]:
        return top_airports_for_each_airport_record_creator(cols)
    if key == tableNames[3]:
        return top_airports_for_each_airport_record_creator(cols)
    if key == tableNames[4]:
        return top_airports_for_each_airport_record_creator(cols)


def top_airports_for_each_airport_record_creator(cols):
    return {'origin_airport': cols[0], 'dest_airport': cols[1], 'average_arrival_delay': cols[2]}


def top_carriers_for_each_airport_record_creator(cols):
    return {'origin_airport': cols[0], 'carrier': cols[1], 'average_arrival_delay': cols[2]}


def best_flight_on_a_given_date_record_creator(cols):
    return {
        'origin_airport': cols[0],
        'dest_airport': cols[1],
        'date': cols[2],
        'am_pm': cols[3],
        'carrier': cols[4],
        'flight_number': cols[5],
        'time': cols[6],
        'average_departure_delay': cols[7]
    }


def average_airport_delay_for_each_airport_record_creator(cols):
    return {'airport': cols[0], 'carrier': cols[1], 'average_departure_delay': cols[2]}


def get_table_name(key):
    table = None
    for tableName in tableNames:
        if key in tableName:
            table = tableName
            break
    return table


def is_airport_id(airport):
    return len(airport) == 3


def record_creator(key, value):
    return {'airport': key, 'count': value}


def lambda_handler(event, context):
    # TODO implement
    # print(str(event))

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    print('bucket => ' + bucket)
    print('key => ' + key)

    # get object
    csv = s3_client.get_object(Bucket=bucket, Key=key)

    csv_body = csv['Body'].read().decode('utf-8')

    line = csv_body.split('\n')

    table_name = get_table_name(key)

    if table_name is None:
        return

    table = dynamodb.get_table_name(table_name)

    for row in line:
        cols = row.split(',')

        if is_airport_id(cols[0]):
            print('key, ' + cols[0])
            record = record_creator_factory(key, cols)
            table.put_item(Item=record)

    return {
        'statusCode': 200,
        'body': json.dumps('Record Created')
    }
