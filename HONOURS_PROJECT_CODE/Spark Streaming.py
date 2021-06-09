# -*- coding: utf-8 -*-
"""
Created on Sun Mar 12 17:53:19 2020

@author: Andrew Gaw
"""
import json
import os


#Import dependencies for Apache Spark
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, FloatType, StructType, StructField
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator


def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
            globals()["sparkSessionSingletonInstance"] = SparkSession \
                .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

class run_stream():
    
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars spark-streaming-kafka-0-8-assembly_2.11-2.4.5.jar pyspark-shell'

    #Create Spark Context
    sc = SparkContext (appName="SparkStreamingHonours")
    sc.setLogLevel("WARN")

    ssc = StreamingContext(sc,40)

    kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'Test4':1})
            
    print ('\n----\Stream Started!')
    parsed = kafkaStream.map(lambda v: json.loads(v[1]))
                    
    #parsed.pprint()
    
    def process(time, rdd):
        print("========= %s =========" % str(time))
        try:
            
        #Get the singleton instance of SparkSession
            spark = getSparkSessionInstance(rdd.context.getConf())
            
            structure = [StructField('Station', StringType(), True),
                         StructField("Public Arrival Time", FloatType(), True),
                         StructField("Working Arrival Time", FloatType(), True),
                         StructField("Public Departure Time", FloatType(), True),
                         StructField("Working Departure Time", FloatType(), True),
                         StructField("Forecast Departure Time", FloatType(), True),
                         StructField("Forecast Arrival Time", FloatType(), True),
                         StructField("Predicted Arrival Time", FloatType(), True)]
                
            final_schema = StructType(fields=structure)
            
            df = spark.read.option("multiLine", True).json(rdd,schema=final_schema)
            
            #df.show()
            
            if (len(df.take(1)) == 0):
                
                print("Dataframe is empty");
                pass;
                
            else:
            
                #df.show()  
            
                df2 = df.drop(df.Station);
                
                df2.show()
                
                vectorAssembler = VectorAssembler(inputCols = ['Public Arrival Time', 'Working Arrival Time', 'Public Departure Time', 'Working Departure Time', 'Forecast Departure Time', 'Forecast Arrival Time'],
                                                  outputCol = 'features')
                
                vector_df = vectorAssembler.transform(df2);
                vector_df = vector_df.select(['features', 'Predicted Arrival Time']);
                
                #vector_df.show(3)
                
                df_split =vector_df.randomSplit([0.6, 0.4])
                train_df = df_split[0]
                test_df = df_split[1]
                
                
                #test_df.show()
                
                
                gbt_model = GBTRegressor(featuresCol = 'features', labelCol = 'Predicted Arrival Time', maxDepth =5, maxIter=10)
                
                model = gbt_model.fit(train_df)
                dt_predictions = model.transform(test_df)
                
                dt_predictions.select("prediction","Predicted Arrival Time").show()
                
                dt_evaluator = RegressionEvaluator(labelCol="Predicted Arrival Time", predictionCol="prediction", metricName="rmse")
                print("Root Mean Squared Error (RMSE) on test data = %g" % dt_evaluator.evaluate(dt_predictions))
        except:
                pass

    parsed.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()