/*
same expense_diary project but here i am using spark to process and ingest data to hdfs from database rather than sqoop and hive which was used in earlier
project. Here i am using hive as a metastore for spark sql.
In this small project user can add day to day expense and it will inserted into database. The user can query the list of expenses he made. 
There is another one important feature where user can know per day what is the overall amount he had spent. For this i will be using spark  
to read data from database and do aggregation on per day basis and export the result back to database which can be queried by user. 
This is like an end to end ETL process. you can schdule the job so that this ETL process will run for fixed interval of time

*/


package org.test.spark
import org.apache.spark.sql._
import scala.util.control.Breaks._
import scala.io.StdIn._
import java.sql.Connection
import java.sql.DriverManager
import scala.util.control.Breaks._
object expense_diary {
  def main(args:Array[String]):Unit={
    val spark=SparkSession.builder().appName("expense diary").master("local[*]")
.config("spark.history.fs.logDirectory", "file:///tmp/spark-events")
.config("spark.eventLog.dir", "file:///tmp/spark-events")
.config("spark.eventLog.enabled", "true")
.config("hive.metastore.uris","thrift://localhost:9083")
.config("spark.sql.warehouse.dir","hdfs://localhost:54310/user/hive/warehouse")
.config("spark.sql.warehouse.dir","hdfs://localhost:54310/user/hive/warehouse")
.config("spark.sql.shuffle.partitions",10)
.config("spark.sql.crossJoin.enabled",true)
.enableHiveSupport()
.getOrCreate();
    breakable{
      while(true){
        println("Hi, press 1 for expense read,2 for expense insert, 3 for expense report and 4 to exit")
        val driver="com.mysql.cj.jdbc.Driver"
        val user="root"
        val password="Root123$"
        val url="jdbc:mysql://localhost/tempdb"
        val tablename="tblexpense"
        val number=readInt()
        if(number==1){
          val read=new diary
          read.mysqlread(spark,driver,url,user,password,tablename)
          
        }
        else if(number==2){
          val write=new diary
          write.mysqlwrite(spark,driver,url,user,password,tablename)
          
        }
        else if(number==3){
          val report=new diary
          report.hivereport(spark,driver,url,user,password,tablename)
        }
        else{
          println("exiting")
          break
        }
      }
    
    }
  }
 }
class diary()
{
  def mysqlread(spark:org.apache.spark.sql.SparkSession,driver:String,url:String,user:String,password:String,tablename:String)={
    
    println("Reading from mysql")
    val readdf=spark.read.format("jdbc").option("url",url).option("driver",driver).option("user",user)
               .option("password",password).option("dbtable",tablename).load()
    readdf.show()
  }
  def mysqlwrite(spark:org.apache.spark.sql.SparkSession,driver:String,url:String,user:String,password:String,tablename:String)
  {
    println("Enter the expense item u want to add")
    val item=readLine()
    println("Enter the amount for the item")
    val amount=readInt()
    val con=DriverManager.getConnection(url,user,password)
    val statement=con.createStatement()
    statement.executeUpdate(s"insert into tblexpense(expensename,expenseamount) values('$item',$amount)")
    println("data written to the expense table")
    
  }
  def hivereport(spark:org.apache.spark.sql.SparkSession,driver:String,url:String,user:String,password:String,tablename:String)={
    val readdf=spark.read.format("jdbc").option("url",url).option("driver",driver).option("user",user)
               .option("password",password).option("dbtable",tablename).load()
    println("data read, processing the report")
    readdf.createOrReplaceTempView("dbtable")
    spark.sql("create database if not exists expense_Stg")
    spark.sql("use expense_stg")
    readdf.write.format("csv").mode("overwrite").saveAsTable("expense_stg.tblexpense_spark")
    spark.sql("create database if not exists expense_curated")
    spark.sql("use expense_curated")
    spark.sql("drop table if exists expense_curated.summary_spark")
    spark.sql("""create external table expense_curated.summary_spark(expense_date date,expense_amt int) row format delimited
      fields terminated by ',' location '/user/hduser/expense_spark'""")
    spark.sql("use expense_stg") 
    println("The aggregated report:")
    spark.sql("""insert overwrite table expense_curated.summary_spark select e.exp_date,sum(expenseamount) from 
      (select from_unixtime(unix_timestamp(expensedate,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd') as exp_date,expenseamount from tblexpense_spark)e group by e.exp_date""")
    spark.sql("select * from expense_curated.summary_spark").show() 
     
    
  
  }
  
}
