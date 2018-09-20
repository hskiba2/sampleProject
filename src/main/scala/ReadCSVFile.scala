import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object ReadCSVFile {
  //It has been created for modeling data. You can say for mapping data with the schema.
  case class Employee(empno:String, ename:String, designation:String, manager:String, hire_date:String, sal:String , deptno:String)

  def main(args : Array[String]): Unit = {
    var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //textFile() - function to load the dataset into RDD as a text file format
    val textRDD = sc.textFile("src\\main\\resources\\emp_data.csv")
    //println(textRDD.foreach(println)
    // map - function is used to map data set value with created case class Employee.
    val empRdd = textRDD.map {
      line =>
        val col = line.split(",")
        Employee(col(0), col(1), col(2), col(3), col(4), col(5), col(6))
    }
    // toDF - function is used to transform RDD to Data Frame.
    val empDF = empRdd.toDF()
    empDF.show()
    /* Spark 2.0 or up
      val empDF= sqlContext.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("src\\main\\resources\\emp_data.csv")
     */
  }
}
