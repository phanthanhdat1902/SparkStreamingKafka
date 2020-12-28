from kafka import KafkaProducer
import requests
producer = KafkaProducer(bootstrap_servers='node-master:9092')
#SBD='02074715'
SBD='02000000'
i=1
while True:
 SBD=SBD[:len(SBD)-len(str(i))]+str(i)
 x = requests.get('https://thanhnien.vn/ajax/diemthi.aspx?kythi=THPT&nam=2020&city=&text='+SBD+'&top=no')
 if x.text=="\n":
     break
 producer.send('RHUST_data', bytes(x.text,'utf-8'))
 producer.flush()
 i=i+1
 print(SBD)
