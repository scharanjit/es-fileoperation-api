# es-fileoperations-api
This is a scala code to extract data from csv files & convert data into JSON format & upload it to on Elastic Search 

Readme is under developement


**Install ElasticSearch**
```
1  wget https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.0.1.deb
2  sudo dpkg -i elasticsearch-1.0.1.deb
3  sudo update-rc.d elasticsearch defaults 95 10
```


**Configure ElasticSearch**
````
1 df
2 sudo vi /etc/elasticsearch/elasticsearch.yml
    uncomment: bootstrap.mlockall: true
    change to you location: path.data: /mnt/elasticsearch/data
3 cd /mnt
4 sudo mkdir elasticsearch
5 cd elasticsearch/
6 sudo mkdir data
7 cd data
8 sudo mkdir elasticsearch
9 sudo chmod 777 /mnt/elasticsearch/data
10 sudo chmod 777 /mnt/elasticsearch/data/elasticsearch
````

**Setup memory for ES (there are few ways... you could also edit ~/.profile)**
````
1 sudo vi /usr/share/elasticsearch/bin/elasticsearch
  add (more or less depnding on your server capabilities): 
    export ES_MIN_MEM=60G
    export ES_MAX_MEM=60G
````  
**Install ES plugins**
````
1 cd /usr/share/elasticsearch
2 sudo bin/plugin --install mobz/elasticsearch-head
3 sudo bin/plugin --install lukas-vlcek/bigdesk
4 sudo bin/plugin -i elasticsearch/marvel/latest

````
**Run ES**
````
1 sudo /etc/init.d/elasticsearch start
2 sudo /etc/init.d/elasticsearch stop
3 sudo /etc/init.d/elasticsearch restart
4 tail -f /var/log/elasticsearch/elasticsearch.log
````

**How To Upload data on ES**

**Creating the user index.**


Since we are dealing with one single node in my example the replicas are set to 0
if you had more than 1 node in the cluster the default for number of replicas is 1 
and will actually put an exact replica of your data on the second node. The shards 
by default is 5 and this basically how elasticsearch can quickly find your data by 
sharding it out to multiple nodes. 

````
 curl -XPUT http://localhost:9200/filo1?pretty=true -d '
   {
       "settings" : {
           "index" : {
               "number_of_shards" : 5,
               "number_of_replicas" : 0
           }
       }
   }
   '
````




**Create the mapping for the user index and type of profile.**

````
curl -XPUT http://localhost:9200/filo1/_mapping/profile?pretty=true -d '
{
    "profile" : {
        "properties" : {
“$outer” :{"type" : "string" , "store" :false},
“Empcode”: { "type" : "string", "store" : true },
“InDate” : { "type" : "string", "store" : true },
“OutDate” : { "type" : "string", "store" : true },
“TotalHours” : { "type" : "string", "store" : true },
“sitedescription” : { "type" : "string", "store" : true },
“ItemDescription” : { "type" : "string", "store" : true }
        }
    }
}
'
````

The JSON format data will be like below
````
****************************************************
{"$outer":{},"Empcode":"10478","InDate":"2016-05-01 08:49:05","OutDate":"2016-05-01 17:56:06","TotalHours":"9H:7M","sitedescription":"Pramati","ItemDescription":"PAJARI MAHESH KUMAR"}
****************************************************
````
So to upload this data  , the URL will be like this.

http://localhost:9200/filo1/profile/

***RUN the project***

```
1.)Clone the project to your local
2.) Go to project folder
3.) sbt compile
4.) sbt "run pathContainingCSVFiles"  //two csv files are present in resources for testing purpose


```