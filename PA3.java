import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PA3
{
  public static void main(String[] args) throws ClassNotFoundException
  {
    SparkSession spark = SparkSession
    .builder()
    .appName("Java Spark SQL basic example")
    .config("spark.master", "local")
    .getOrCreate();

    spark.sparkContext().setLogLevel("WARN");

    // A JSON dataset is pointed to by path.
    // The path can be either a single text file or a directory storing text files
    Dataset<Row> reservations = spark.read().json("reservations.json");

    // The inferred schema can be visualized using the printSchema() method
    reservations.printSchema();
    // root
    //  |-- age: long (nullable = true)
    //  |-- name: string (nullable = true)

    // Creates a temporary view using the DataFrame
    reservations.createOrReplaceTempView("reservations");

    // SQL statements can be run by using the sql methods provided by spark
    Dataset<Row> allDF = spark.sql("SELECT r.* FROM reservations AS r"); // SELECT everything. you will see that some output is in arrays
    allDF.show();


    Dataset<Row> namesDF = spark.sql("SELECT r.boat.name FROM reservations AS r"); // SELECT the boat name in every reservation
    namesDF.show();


    System.out.println("=== MARKS START OF STUDENT OUTPUT. DO NOT REMOVE ===");
    // ======== YOUR CODE HERE ==========



    // ======== END YOUR CODE  ==========
  }
}
