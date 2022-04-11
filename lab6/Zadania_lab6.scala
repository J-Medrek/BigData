// Databricks notebook source
import org.apache.spark.sql.types.{
    StructType, StructField, DateType,StringType,IntegerType,DecimalType,DataTypes,ShortType,VarcharType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col

// COMMAND ----------

val DecimalType = DataTypes.createDecimalType(8, 2)
val schema = StructType( StructField("AccountID", IntegerType, true) :: StructField("TranDate", DateType, false) :: StructField("TranAmt",IntegerType, false):: Nil )
val schema2 = StructType( StructField("RowID", IntegerType, true) :: StructField("FName", StringType, true) :: StructField("Salary",IntegerType, false):: Nil )

// COMMAND ----------

import java.sql.Date

val tranData = Seq(Row( 1,Date.valueOf("2011-01-01"), 500),
   Row(  1,Date.valueOf("2011-01-15"), 50),
Row(  1,Date.valueOf("2011-01-22"), 250),
Row(  1,Date.valueOf("2011-01-24"), 75),
Row(  1,Date.valueOf("2011-01-26"), 125),
Row(  1,Date.valueOf("2011-01-28"), 175),
Row(  2,Date.valueOf("2011-01-01"), 500),
Row(  2,Date.valueOf("2011-01-15"), 50),
Row(  2,Date.valueOf("2011-01-22"), 25),
Row(  2,Date.valueOf("2011-01-23"), 125),
Row(  2,Date.valueOf("2011-01-26"), 200),
Row(  2,Date.valueOf("2011-01-29"), 250),
Row(  3,Date.valueOf("2011-01-01"), 500),
Row(  3,Date.valueOf("2011-01-15"), 50 ),
Row(  3,Date.valueOf("2011-01-22"), 5000),
Row(  3,Date.valueOf("2011-01-25"), 550),
Row(  3,Date.valueOf("2011-01-27"), 95 ),
Row(  3,Date.valueOf("2011-01-30"), 2500)
  )


// COMMAND ----------


val logicalData = Seq(
   Row(1,"George", 800),
Row(2,"Sam", 950),
Row(3,"Diane", 1100),
Row(4,"Nicholas", 1250),
Row(5,"Samuel", 1250),
Row(6,"Patricia", 1300),
Row(7,"Brian", 1500),
Row(8,"Thomas", 1600),
Row(9,"Fran", 2450),
Row(10,"Debbie", 2850),
Row(11,"Mark", 2975),
Row(12,"James", 3000),
Row(13,"Cynthia", 3000),
Row(14,"Christopher", 5000)
  )


// COMMAND ----------

val Transactions = spark.createDataFrame(spark.sparkContext.parallelize(tranData),schema)
val Logical =  spark.createDataFrame(spark.sparkContext.parallelize(logicalData),schema2)

// COMMAND ----------

display(Transactions)

// COMMAND ----------

display(Logical)

// COMMAND ----------


val windowSpec = Window
  .partitionBy("AccountId")
  .orderBy("TranDate")
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)


display(Transactions.select("AccountID","TranDate","TranAmt").withColumn("RunTotalAmt",sum("TranAmt") over windowSpec).orderBy("AccountID","TranDate"))
        

// COMMAND ----------

val windowSpec = Window
  .partitionBy("AccountId")
  .orderBy("TranDate")
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)


display(Transactions.select("AccountID","TranDate","TranAmt")
        .withColumn("RunAvg",avg("TranAmt") over windowSpec)
        .withColumn("RunTranQty",count("*") over windowSpec)
        .withColumn("RunSmallAmt",min("TranAmt") over windowSpec)
        .withColumn("RunLargeAmt,",max("TranAmt") over windowSpec)
        .withColumn("RunTotalAmt,",sum("TranAmt") over windowSpec)
        .orderBy("AccountID","TranDate"))

// COMMAND ----------

val windowSpec = Window
  .partitionBy("AccountId")
  .orderBy("TranDate")

display(Transactions.select("AccountID","TranDate","TranAmt")
        .withColumn("Slidevg",avg("TranAmt") over windowSpec.rowsBetween(-2, Window.currentRow))
        .withColumn("SlideTranQty",count("*") over windowSpec.rowsBetween(-2, Window.currentRow))
        .withColumn("SlideSmallAmt",min("TranAmt") over windowSpec.rowsBetween(-2, Window.currentRow))
        .withColumn("SlideLargeAmt,",max("TranAmt") over windowSpec.rowsBetween(-2, Window.currentRow))
        .withColumn("SlideTotalAmt,",sum("TranAmt") over windowSpec.rowsBetween(-2, Window.currentRow))
        .withColumn("RN",row_number.over(windowSpec))
        .orderBy("AccountID","TranDate"))

// COMMAND ----------

val windowSpec = Window
  .orderBy("Salary")
  
display(Logical.select("RowID","FName","Salary")
        .withColumn("SumByRows",sum("Salary") over windowSpec.rowsBetween(Window.unboundedPreceding, Window.currentRow))
        .withColumn("SumByRange",sum("Salary") over windowSpec.rangeBetween(Window.unboundedPreceding, Window.currentRow))
        .orderBy("RowID"))

// COMMAND ----------

val header = spark.read.format("delta").load("dbfs:/FileStore/lab4/delta/SalesOrderHeader")

val windowSpec = Window
  .partitionBy("AccountNumber")
  .orderBy("OrderDate")
  
display(header.select("AccountNumber","OrderDate","TotalDue")
        .withColumn("RN",row_number() over windowSpec.rowsBetween(Window.unboundedPreceding, Window.currentRow))
        .orderBy("AccountNumber").limit(10))

// COMMAND ----------

//Zadanie 2
val header = spark.read.format("delta").load("dbfs:/FileStore/lab4/delta/SalesOrderHeader")
val detail = spark.read.format("delta").load("dbfs:/FileStore/lab4/delta/SalesOrderDetail")

// COMMAND ----------

display(header)

// COMMAND ----------

val windowSpec = Window
  .partitionBy("SalesOrderID")
  .orderBy(col("OrderQty").desc)

// COMMAND ----------

display(detail.select("SalesOrderID","OrderQty")
        .withColumn("RN",row_number() over windowSpec.rowsBetween(Window.unboundedPreceding, Window.currentRow))
        .withColumn("Dense_Rank",dense_rank() over windowSpec.rowsBetween(Window.unboundedPreceding, Window.currentRow))
        .withColumn("Rank",rank() over windowSpec.rowsBetween(Window.unboundedPreceding, Window.currentRow))
        .withColumn("Lag",lag("OrderQty",2).over(windowSpec))
        .withColumn("Lead",lead("OrderQty",2).over(windowSpec))
        .withColumn("First",first("OrderQty") over windowSpec.rowsBetween(Window.unboundedPreceding, Window.currentRow))
        .withColumn("Last",first("OrderQty") over windowSpec.rowsBetween(Window.unboundedPreceding, Window.currentRow))
       )

// COMMAND ----------

display(detail.select("SalesOrderID","OrderQty")
        .withColumn("RN",row_number() over windowSpec)
        .withColumn("Dense_Rank",dense_rank() over windowSpec)
        .withColumn("Rank",rank() over windowSpec)
        .withColumn("Lag",lag("OrderQty",2) over windowSpec)
        .withColumn("Lead",lead("OrderQty",2) over windowSpec)
        .withColumn("First",first("OrderQty") over windowSpec.rangeBetween(Window.unboundedPreceding,Window.currentRow))
        .withColumn("Last",first("OrderQty") over windowSpec.rangeBetween(Window.unboundedPreceding,Window.currentRow))
       )

// COMMAND ----------

//Zadanie 3

val joinExpression = detail.col("SalesOrderID") === header.col("SalesOrderID")
display(detail.join(header, joinExpression, "left_semi"))

// COMMAND ----------

detail.join(header, joinExpression, "left_semi").explain(mode="formatted")

// COMMAND ----------

val newRow=Seq((1,null,null,null,null,null,null,null,null)).toDF()
val joinExpression = detail.union(newRow.toDF).col("SalesOrderID") === header.col("SalesOrderID")
display(detail.union(newRow.toDF).join(header, joinExpression, "left_anti"))

// COMMAND ----------

detail.union(newRow.toDF).join(header, joinExpression, "left_anti").explain(mode="formatted")

// COMMAND ----------

//Zadanie 4
val joinExpression = detail.col("SalesOrderID") === header.col("SalesOrderID")
display(detail.join(header, joinExpression, "inner").drop(header.col("SalesOrderID")))


// COMMAND ----------

display(detail.join(header, "SalesOrderID"))

// COMMAND ----------

//Zadanie 5

import org.apache.spark.sql.functions.broadcast
display(detail.join(broadcast(header), joinExpression))


// COMMAND ----------

detail.join(broadcast(header), joinExpression).explain(mode="formatted")
