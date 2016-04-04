package com.spark.dataframes

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import scala.reflect.runtime.universe

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
    val firstName = persons.toDF()
    // How many persons with different fnames ? 
    val count = firstName.select("fName").distinct.count
    System.out.println(count)
    


  }
}