package spark.streaming;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;


public class FileStructuredStreaming {

    public static void main(String[] args) throws Exception {

       // System.setProperty("hadoop.home.dir", "c:/winutils");

        //build the spark sesssion
        SparkSession spark = SparkSession.builder()
                .master("local[3]")
                .appName("spark streaming")
                .config("spark.sql.warehouse.dir", "file:///app/").getOrCreate();

        //set the log level only to log errors
        spark.sparkContext().setLogLevel("ERROR");

        //define schema type of file data source
        StructType schema = new StructType()
                .add("empId", DataTypes.StringType)
                .add("empName", DataTypes.StringType)
                .add("department", DataTypes.StringType);


        //build the streaming data reader from the file source, specifying csv file format
        Dataset<Row> rawData = spark.readStream().option("header", true)
                .format("json")
                .schema(schema)
                .csv("C:/Users/lenovo/streamingfiles/*.json");
        rawData.printSchema();

        rawData.createOrReplaceTempView("empData");

        //count of employees grouping by department
        Dataset<Row> result = spark.sql("select empId from empData");

        //write stream to output console with update mode as data is being aggregated
        StreamingQuery query = result.writeStream().outputMode(OutputMode.Update()).format("console").start();
        query.awaitTermination();
    }
}