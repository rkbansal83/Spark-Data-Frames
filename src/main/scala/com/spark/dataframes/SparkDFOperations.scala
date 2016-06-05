package com.spark.dataframes

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

/**
 * @author RAVI
 This class is to demonstrate the left join operation using SPARK SQL/hive QL APIs. 
 */
object SparkDFOperations {

	def main(args: Array[String]) {

		val conf = new SparkConf().setAppName("SparkDFOperations")
		val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)

    
    hiveContext.sql("DROP TABLE IF EXISTS employee");
    hiveContext.sql("CREATE EXTERNAL TABLE IF NOT EXISTS employee(id INT, name STRING, deptId INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' LOCATION \'"+args(0)+'\'')
    val empDF = hiveContext.sql("FROM employee SELECT id, name, deptId")
    empDF.show();
    
    hiveContext.sql("DROP TABLE IF EXISTS department");
    hiveContext.sql("CREATE EXTERNAL TABLE IF NOT EXISTS department(deptId INT, name STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' LOCATION \'"+args(1)+'\'')
    val deptDF = hiveContext.sql("FROM department SELECT deptId, name")
    deptDF.show();
    
    val deptEmpDF = hiveContext.sql("SELECT d.deptId, d.name, e.id, e.name FROM department d LEFT JOIN employee e ON (d.deptId=e.deptId)")
    deptEmpDF.show()
    sc.stop();
	}
}