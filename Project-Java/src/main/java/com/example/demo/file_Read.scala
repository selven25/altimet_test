package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import scala.io.Source

object readfile {

	def main(args:Array[String]):Unit={

			println("===read the source file which is unzipped from s3 location====")

			val conf = new SparkConf().setAppName("first").setMaster("local[*]")
			 .set("spark.driver.host","localhost")
			 .set("spark.driver.allowMultipleContexts", "true")

			val sc = new SparkContext(conf)

			sc.setLogLevel("ERROR")

			val spark = SparkSession.builder.getOrCreate()

			import spark.implicits._

		
    val filePath = "/home/ec2-user/environment/6607a2044a268214da315eee-002/source_file.txt"
    val df = spark.read.text(filePath)
    df.show()

 val jdbcUrl = "jdbc:redshift://6607a2044a268214da315eee-002-redshift-cluster-endpoint:5439/your-database"
    val username = "6607a2044a268214da315eee-002"
    val password = "/home/ec2-user/environment/6607a2044a268214da315eee-002/password_file"
 var connection: Connection = null
 var preparedStatement: PreparedStatement = null
 
 
 try {
     
      Class.forName("com.amazon.redshift.jdbc.Driver")

    
      connection = DriverManager.getConnection(jdbcUrl, username, password)


      preparedStatement = connection.prepareStatement(query)
      val rowCount = preparedStatement.executeUpdate()

      println(s"Data loaded successfully. Rows affected: $rowCount")
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {

      if (preparedStatement != null) preparedStatement.close()
      if (connection != null) connection.close()
    }

    spark.stop()
  }
}
