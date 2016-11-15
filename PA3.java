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

    Dataset<Row> q1 = spark.sql("SELECT 'ANSWER Q1 HERE'");
    Dataset<Row> q2 = spark.sql("SELECT 'ANSWER Q2 HERE'");
    Dataset<Row> q3 = spark.sql("SELECT 'ANSWER Q3 HERE'");
    Dataset<Row> q4 = spark.sql("SELECT 'ANSWER Q4 HERE'");
    Dataset<Row> q5 = spark.sql("SELECT 'ANSWER Q5 HERE'");
    Dataset<Row> q6 = spark.sql("SELECT 'ANSWER Q6 HERE'");
    Dataset<Row> q7 = spark.sql("SELECT 'ANSWER Q7 HERE'");

    // ======== END YOUR CODE  ==========

    System.out.println("=== QUESTION 1 ===");
    q1.show();
    System.out.println("=== QUESTION 2 ===");
    q2.show();
    System.out.println("=== QUESTION 3 ===");
    q3.show();
    System.out.println("=== QUESTION 4 ===");
    q4.show();
    System.out.println("=== QUESTION 5 ===");
    q5.show();
    System.out.println("=== QUESTION 6 ===");
    q6.show();
    System.out.println("=== QUESTION 7 ===");
    q7.show();
  }
}
