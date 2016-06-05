package com.spark.dataframes

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.Column
import java.util.Calendar
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StringType
import java.text.SimpleDateFormat

//define the schema using a case class
case class Person(fName: String, lName:String, Age: Integer)

object SparkDFPersons{
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("SparkDFPersons")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

 
   import sqlContext.implicits._
   import sqlContext._
  
   // load the data into an RDD
    val personsText = sc.textFile(args(0))

    // create an RDD of Person objects 
    val persons = personsText.map(_.split(",")).map(p => Person(p(0), p(1),p(2).toInt))

    // change  RDD of Person objects to a DataFrame
    val personDF = persons.toDF()
        
    //print the contents of person data frames.
    personDF.show();
    
    val updatedBy = "USER"; //Assuming USER is the last updated user.
    val now = Calendar.getInstance().getTime(); //Assuming the curent time stamp is last updated timestamp.
    
    val personDF1 = personDF.withColumn("last_modified_by", lit(updatedBy).cast(StringType));
    val personDF2 = personDF1.withColumn("last_modified_tms", lit(now.toString()).cast(StringType));
    
    //we can use below approach as well in case we want new column type to be of timestamp.
    //val personDF2 = personDF1.withColumn("last_modified_tms", from_unixtime(unix_timestamp()).cast(TimestampType));
    
    personDF2.show();
    personDF2.printSchema();
    
    sc.stop();


  }
}