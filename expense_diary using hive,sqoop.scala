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
