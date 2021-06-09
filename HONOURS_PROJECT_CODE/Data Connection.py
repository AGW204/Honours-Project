#
# National Rail Open Data client demonstrator
# Copyright (C)2019 OpenTrainTimes Ltd.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

#
# This code was First Modified on 26th February 2019 and was done so following the terms of the GNU Public Version 3
# License is available at <https://www.gnu.org/licenses/gpl-3.0.txt/>
#
import stomp
import zlib
import io
import time
import socket
import json
import xml.etree.ElementTree as ET
from json import loads


#Import dependencies for Kafka Producer/Consumer
from kafka import KafkaProducer
from kafka import KafkaConsumer


USERNAME = #Account details removed due to data protection and privacy concerns. a new account can be made at https://opendata.nationalrail.co.uk/
PASSWORD = #See Above Comment
HOSTNAME = 'darwin-dist-44ae45.nationalrail.co.uk'
HOSTPORT = 61613
# Always prefixed by /topic/ (it's not a queue, it's a topic)
TOPIC = '/topic/darwin.pushport-v16'

CLIENT_ID = socket.getfqdn()
HEARTBEAT_INTERVAL_MS = 1500000
RECONNECT_DELAY_SECS = 15

if USERNAME == '':
    raise Exception("Please configure your username and password in opendata-nationalrail-client.py!")


def connect_and_subscribe(connection):

    if stomp.__version__[0] < 5:
        connection.start()

    connect_header = {'client-id': USERNAME + '-' + CLIENT_ID}
    subscribe_header = {'activemq.subscriptionName': CLIENT_ID}

    connection.connect(username=USERNAME,
                       passcode=PASSWORD,
                       wait=True,
                       headers=connect_header)

    connection.subscribe(destination=TOPIC,
                         id='1',
                         ack='auto',
                         headers=subscribe_header)


class StompClient(stomp.ConnectionListener):

    def on_heartbeat(self):
        print('Received a heartbeat')

    def on_heartbeat_timeout(self):
        print('ERROR: Heartbeat timeout')

    def on_error(self, headers, message):
        print('ERROR: %s' % message)

    def on_disconnected(self):
        print('Disconnected waiting %s seconds before exiting' % RECONNECT_DELAY_SECS)
        time.sleep(RECONNECT_DELAY_SECS)
        exit(-1)

    def on_connecting(self, host_and_port):
        print('Connecting to ' + host_and_port[0])

    def on_message(self, headers, message):
        try:
            # print('\n----\nGot a message!\n\t%s' % message)
            bio = io.BytesIO()
            bio.write(str.encode('utf-16'))
            bio.seek(0)
            msg = zlib.decompress(message, zlib.MAX_WBITS | 32)
            xml_string = msg.decode()
            
            
            #print (xml_string)
            
            root = ET.fromstring(xml_string)
            
            for location in root.iter('{http://www.thalesgroup.com/rtti/PushPort/Forecasts/v3}Location'):
                
                if (location.get('tpl') != None and location.get('pta') != None and location.get('wta') != None and location.get('ptd') != None and location.get('wtd') != None):
                
                    #pulls Station from location Feed
                    station = location.get('tpl')
                    
                    #Pulls other Relevant Variables from the feed and removes ":" from the data, replacing it with "."
                    pub_arr = location.get('pta').replace(':','.')
                    work_arr = location.get('wta').replace(':','.')
                    pub_dep = location.get('ptd').replace(':','.')
                    work_dep = location.get('wtd').replace(':','.')
                
                    #Checks if the Variable has a seconds value and if so, converts this to suitable decimal equivalent 
                    if (len(work_arr) == 8):
                        
                        work_arr = work_arr[:-3] + '50'
                        
                    
                    if (len(work_dep) == 8):
                        work_dep = work_dep[:-3] +'50'
                    
                    
                else:
                       pass
                
            for arrival in root.iter('{http://www.thalesgroup.com/rtti/PushPort/Forecasts/v3}arr'):
                
                if (arrival.get('et') != None and arrival.get('wet') != None):
                   
                    #Pulls data required from the Arrival feed and removes ":" from the data, replacing it with "."
                    up_arr = arrival.get('et').replace(':','.')
                    acc_arr = arrival.get('wet').replace(':','.')
                    
                    #Checks if the Variable has a seconds value and if so, converts this to suitable decimal equivalent
                    if (len(up_arr) == 8):
                        
                        up_arr = up_arr[:-3] + '50'
                        
                    if (len(acc_arr) == 8):
                        acc_arr = acc_arr[:-3] +'50'
                        
                else:
                        pass;
                
            for departure in root.iter('{http://www.thalesgroup.com/rtti/PushPort/Forecasts/v3}dep'):
                
                    
                    #pull data required for Departure updates
                    up_dep = departure.get('et').replace(':','.')
                    
                
                    if (len(up_dep) == 8):
                        
                            up_dep = up_dep[:-3] + '50'
                            
                        
            if (pub_arr is None and work_arr  is None and pub_dep is None and work_dep is None and up_arr is None and acc_arr is None and up_dep is None):
                pass
            
            else:   
                    
                #Formats Data to dictionary and makes variabels more readable
                    Result = {
                                "Public Arrival Time": pub_arr,
                                "Working Arrival Time": work_arr,
                                "Public Departure Time": pub_dep,
                                "Working Departure Time": work_dep,
                                "Forecast Departure Time": up_dep,
                                "Forecast Arrival Time": up_arr,
                                "Predicted Arrival Time": acc_arr
                                }
                    
                    
                    
                    
                    Reformated = {"Station": str, "Public Arrival Time": float, "Working Arrival Time": float, "Public Departure Time": float, "Working Departure Time": float, "Forecast Departure Time": float, "Forecast Arrival Time": float, "Predicted Arrival Time": float, "Predicted Departure Time": float }
                    
                    
                    Formatted_Results = { k:Reformated.get(k,str)(v) for k,v in Result.items()}
                    
                    print(Formatted_Results)
                    
                    
                    
                    producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
                    producer.send('Test4', Formatted_Results)
                    
                    #consumer = KafkaConsumer('Main_Test', bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest', value_deserializer=lambda x: loads(x.decode('utf-8')))
            
                    #for message in consumer:
                        #print(message);
        except:
                pass
            
            
conn = stomp.Connection12([(HOSTNAME, HOSTPORT)],
                          auto_decode=False,
                          heartbeats=(HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS))

conn.set_listener('', StompClient())
connect_and_subscribe(conn)

while True:
    time.sleep(1)

conn.disconnect()






