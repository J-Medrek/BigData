// Databricks notebook source
import org.apache.spark.sql.functions._
val read = (s: String) => {
   spark.read
  .format("jdbc")
  .option("url","jdbc:sqlserver://bidevtestserver.database.windows.net:1433;database=testdb")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("dbtable",s)
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .load()
}

val Customer_t=read("SalesLT.Customer")
val ProductModel_t=read("SalesLT.ProductModel")
val vProductModelCatalogDescription_t=read("SalesLT.vProductModelCatalogDescription")
val ProductDescription_t=read("SalesLT.ProductDescription")
val Product_t=read("SalesLT.Product")
val ProductModelProductDescription_t=read("SalesLT.ProductModelProductDescription")
val vProductAndDescription_t=read("SalesLT.vProductAndDescription")
val ProductCategory_t=read("SalesLT.ProductCategory")
val vGetAllCategories_t=read("SalesLT.vGetAllCategories")
val Address_t=read("SalesLT.Address")
val CustomerAddress_t=read("SalesLT.CustomerAddress")
val SalesOrderDetail_t=read("SalesLT.SalesOrderDetail")
val SalesOrderHeader_t=read("SalesLT.SalesOrderHeader")


// COMMAND ----------

val save = (s: org.apache.spark.sql.DataFrame,str: String) => {
  s.write.format("delta").mode("overwrite").save(s"/FileStore/lab4/delta/${str}")
}

save(Customer_t,"Customer")
save(ProductModel_t,"ProductModel")
save(vProductModelCatalogDescription_t,"vProductModelCatalogDescription")
save(ProductDescription_t,"ProductDescription")
save(Product_t,"Product")
save(ProductModelProductDescription_t,"ProductModelProductDescription")
save(vProductAndDescription_t,"vProductAndDescription")
save(ProductCategory_t,"ProductCategory")
save(vGetAllCategories_t,"vGetAllCategories")
save(Address_t,"Address")
save(CustomerAddress_t,"CustomerAddress")
save(SalesOrderDetail_t,"SalesOrderDetail")
save(SalesOrderHeader_t,"SalesOrderHeader")

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/lab4/delta/Address"))

// COMMAND ----------

val countColumns=(s: org.apache.spark.sql.DataFrame) => 
{
  val cols=s.columns
  cols.map(c=>{sum(when(col(c).isNull,1)).alias(c)})
}

display(ProductModel_t.select(countColumns(ProductModel_t):_*))
display(vProductModelCatalogDescription_t.select(countColumns(vProductModelCatalogDescription_t):_*))
display(ProductDescription_t.select(countColumns(ProductDescription_t):_*))
display(Product_t.select(countColumns(Product_t):_*))
display(ProductModelProductDescription_t.select(countColumns(ProductModelProductDescription_t):_*))
display(vProductAndDescription_t.select(countColumns(vProductAndDescription_t):_*))
display(ProductCategory_t.select(countColumns(ProductCategory_t):_*))
display(vGetAllCategories_t.select(countColumns(vGetAllCategories_t):_*))
display(Address_t.select(countColumns(Address_t):_*))
display(CustomerAddress_t.select(countColumns(CustomerAddress_t):_*))
display(SalesOrderDetail_t.select(countColumns(SalesOrderDetail_t):_*))
display(SalesOrderHeader_t.select(countColumns(SalesOrderHeader_t):_*))
display(Customer_t.select(countColumns(Customer_t):_*))


// COMMAND ----------

val fun =(s : org.apache.spark.sql.DataFrame, d: String, i: Int)=>{s.na.fill(d).na.fill(i) } 

val Customer_t_fun=fun(Customer_t,"Customer",0)
val ProductModel_t_fun=fun( ProductModel_t,"Product",0)
val vProductModelCatalogDescription_t_fun=fun(vProductModelCatalogDescription_t,"CatalogDesc",0)
val ProductDescription_t_fun=fun(ProductDescription_t,"ProdDesc",0)
val Product_t_fun=fun(Product_t,"Product",0)
val ProductModelProductDescription_t_fun=fun(ProductModelProductDescription_t,"ProdDesc",0)
val vProductAndDescription_t_fun=fun(vProductAndDescription_t,"vProduct",0)
val ProductCategory_t_fun=fun(ProductCategory_t,"ProdCatego",0)
val vGetAllCategories_t_fun=fun(vGetAllCategories_t,"AllCateg",0)
val Address_t_fun=fun(Address_t,"Address",0)
val CustomerAddress_t_fun=fun(CustomerAddress_t,"CustAddress",0)
val SalesOrderDetail_t_fun=fun(SalesOrderDetail_t,"SalesOrderDet",0)
val SalesOrderHeader_t_fun=fun(SalesOrderHeader_t,"SalesOrderHead",0)


// COMMAND ----------

//Użyj funkcji drop żeby usunąć nulle

val drop_null=(s: org.apache.spark.sql.DataFrame) => {s.na.drop()}

val Customer_t_drop=drop_null(Customer_t)
val ProductModel_t_drop=drop_null( ProductModel_t)
val vProductModelCatalogDescription_t_drop=drop_null(vProductModelCatalogDescription_t)
val ProductDescription_t_drop=drop_null(ProductDescription_t)
val Product_t_drop=drop_null(Product_t)
val ProductModelProductDescription_t_drop=drop_null(ProductModelProductDescription_t)
val vProductAndDescription_t_drop=drop_null(vProductAndDescription_t)
val ProductCategory_t_drop=drop_null(ProductCategory_t)
val vGetAllCategories_t_drop=drop_null(vGetAllCategories_t)
val Address_t_drop=drop_null(Address_t)
val CustomerAddress_t_drop=drop_null(CustomerAddress_t)
val SalesOrderDetail_t_drop=drop_null(SalesOrderDetail_t)
val SalesOrderHeader_t_drop=drop_null(SalesOrderHeader_t)

// COMMAND ----------

//wybierz 3 dowolne funkcje agregujące i policz dla TaxAmt, Freight, [SalesLT].[SalesOrderHeader]

display(SalesOrderHeader_t.agg(min("Freight"),min("TaxAmt"),avg("Freight"),avg("TaxAmt"),max("Freight"),max("TaxAmt")))

// COMMAND ----------

val x=Product_t.groupBy($"ProductCategoryID").agg(countDistinct($"Color"))
val y=Product_t.groupBy($"Color").agg(mean($"StandardCost"))
val z=Product_t.groupBy($"ProductModelId").agg(max($"StandardCost"))

display(x)

// COMMAND ----------

val map1=Customer_t.map( f => f.getString(2) +" "+ f.getString(3)+ " "+f.getString(5));
val map2=Customer_t.map( f => (f.getString(8)).split('\\')(1));
val map3=SalesOrderDetail_t.map( f => BigDecimal(f.getShort(2))*f.getDecimal(4));
val map4=Customer_t_drop.map(row => row.getString(13).replaceAll("-","|"))
display(map4)

// COMMAND ----------

//Zadanie 2

val add50UDF = udf { s: Int => s+50 }
val convertToIntUDF = udf { s: Double => s.toInt }
val getFirstPartUDF = udf { s: String => s.split("-")(0) }

display(SalesOrderHeader_t.select($"SalesOrderID",add50UDF($"SalesOrderID"),$"TaxAmt",convertToIntUDF($"TaxAmt"),$"rowguid",getFirstPartUDF($"rowguid")))

// COMMAND ----------

//Zadanie 3
import sys.process._
import java.net.URL
import java.nio.file.{Files, Paths, StandardCopyOption}

def getfile(url: String, outputFile: String):Unit = {
  val in = new URL(url).openStream
  val out = Paths.get("/tmp/" + outputFile)
  Files.copy(in, out, StandardCopyOption.REPLACE_EXISTING)
}

val filePath = "/FileStore/tables/Files/"
val url="https://raw.githubusercontent.com/J-Medrek/BigData/main/lab4/brzydki.json"
val file = "brzydki.json"
val dbfsdestination = "dbfs:/FileStore/"
val tmp = "file:/tmp/"

dbutils.fs.mkdirs(filePath)

getfile(url,file)
dbutils.fs.mv(tmp + file, dbfsdestination + file)
val brzydkijson = spark.read.format("json")
            .option("multiline", "true")
            .load("dbfs:/FileStore/brzydki.json")

// COMMAND ----------

brzydkijson.printSchema()

// COMMAND ----------

val properties=brzydkijson.select(explode($"features.properties")).select($"col.*")
val z=properties.select("*","baseFormComponent.*").drop("baseFormComponent")
display(z)
