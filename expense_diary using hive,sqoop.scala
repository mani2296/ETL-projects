/* DESCRIPTION
In this small project user can add day to day expense and it will inserted into mysql database. The user can query the list of expenses he made. 
There is another one important feature where user can know per day what is the overall amount he had spent. For this i will be using sqoop to import the data into hdfs and 
using hive i can do aggregation on per day basis and export the result back to mysql database using sqoop which can be queried by user. 
This is like an end to end ETL process. you can schdule the job so that this ETL process will run for fixed interval of time

*/
package com.org.excercise
import scala.io.StdIn._
import java.sql.Connection
import java.sql.DriverManager
import scala.util.control.Breaks._


object personalexpense_diary {
  def main(args:Array[String])={
    breakable{
      while(true){
        println("Hi, press 1 for adding expense, 2 for reading the expense,3 for expense report,4 to exit")
        val user_input=readInt()
        if(user_input==1){
          println("enter the expense name")
          val expensename=readLine()
          println("enter the expense amount")
          val expenseamount=readInt()
          val add_object=new diary
          add_object.mysqlinsert(expensename, expenseamount)
            }
        else if(user_input==2){
          val read_object=new diary
          read_object.mysqlread()
          }
        else if(user_input==3){
          val report=new diary
          report.mysqlquery()
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
  def mysqlinsert(name:String,amt:Int)={
    val con=DriverManager.getConnection("jdbc:mysql://localhost/tempdb","root","Root123$")
    val statement=con.createStatement()
    statement.executeUpdate(s"insert into tblexpense(expensename,expenseamount) values('$name',$amt)")
    println("data written to the expense table")
}
  def mysqlread()={
    val con=DriverManager.getConnection("jdbc:mysql://localhost/tempdb","root","Root123$")
    val statement=con.createStatement()
    val expensedata=statement.executeQuery("select * from tblexpense")
    while(expensedata.next()){
      println("expense name:"+expensedata.getString("expensename"))
      println("expense amount:"+expensedata.getInt("expenseamount"))
      println("expense date:"+expensedata.getTimestamp("expensedate"))
      println("==========")
    }
    println("End")
  }
  def mysqlquery()={
    
    val conn=DriverManager.getConnection("jdbc:mysql://localhost/tempdb","root","Root123$")
    val statement=conn.createStatement()
    val summary=statement.executeQuery("select * from expense_summary")
    while(summary.next()){
      println("date: "+summary.getString("expense_date"))
      println("total amount spent: "+summary.getInt("total_amount"))
      println("==========")
   
    }
     println("End")
      
    }
}

//sqoop import to hdfs
/* 

sqoop job --create expense_job -- import --connect jdbc:mysql://localhost/tempdb --username root --password Root123$ --table tblexpense 
--target-dir /user/hduser/tblexpense_stg --incremental append --check-column id --last-value 0 -m1 --driver com.mysql.cj.jdbc.Driver

*/

//processing in hive
/*
create external table tblexpense_raw(id int,expense_item string,expense_amount int,expense_date timestamp) row format delimited fields terminated by ',' 
location '/user/hduser/tblexpense_stg';

create external table exp_summary(expense_date date,total_amount int) row format delimited fields terminated ',' location /user/hduser/exp_summary;

insert overwrite table exp_summary select e.exp_date,sum(expense_amount) from (select from_unixtime(unix_timestamp(expense_date,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd') as exp_date,
expense_amount from tblexpense_raw)e group by e.exp_date;

*/

//sqoop export

/*
sqoop job --create expense_export -- export --connect jdbc:mysql://localhost/tempdb --username root --password Root123$ --table expense_summary \
--export-dir /user/hduser/exp_summary -m1
*/

