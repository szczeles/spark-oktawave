# spark-oktawave

Tool for launching Apache Spark clusters in Oktawave cloud.

Install using:

    pip3 install https://github.com/szczeles/spark-oktawave/archive/master.zip


## Cluster monitoring

In order to watch cluster resources usage realtime 
you can install graphite&grafana dashboard. The
easiest way to get it is to use kamon grafana-graphite 
solution as follows:

    git clone https://github.com/kamon-io/docker-grafana-graphite.git 
    cd docker-grafana-graphite
    docker build -t my_grafana .

    # set just built image in docker-compose
    sed -i 's/image: .*/image: my_grafana/' docker-compose.yml 
    
    # disable image pull in Makefile since we use own image now
    sed -i 's/up : prep pull/up: prep/' Makefile 

Then configure Grafana (runnning on 80 port) to use graphite
datasource. Graphite url is `http://localhost:81` and access
mode should be `proxy`. Import [spark-oktawave dashboard](https://raw.githubusercontent.com/szczeles/spark-oktawave/master/grafana-dashboard.json)
in grafana.

Finally install collectd on cluster nodes using:

    spark_oktawave enable_monitoring [cluster_name] [graphite_host]

And enjoy live cluster monitoring!
