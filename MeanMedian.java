
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class MeanMedian {
	public static void main(String[] args) {
		// file paths
		String trainDataSet = "train.csv";
		String testDataSet = "test.csv";
		
		// confeguring spark
		SparkConf conf = new SparkConf().setMaster("local").setAppName(
				"Bimbo miner");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		// header for train data
		StructType trainDataSchema = new StructType(new StructField[] {
				new StructField("week_number", DataTypes.IntegerType, true,
						Metadata.empty()),
				new StructField("sales_depot_id", DataTypes.IntegerType, true,
						Metadata.empty()),
				new StructField("sales_channel_id", DataTypes.IntegerType,
						true, Metadata.empty()),
				new StructField("route_id", DataTypes.IntegerType, true,
						Metadata.empty()),
				new StructField("client_id", DataTypes.IntegerType, true,
						Metadata.empty()),
				new StructField("product_id", DataTypes.IntegerType, true,
						Metadata.empty()),
				new StructField("sales_units", DataTypes.IntegerType, true,
						Metadata.empty()),
				new StructField("sales", DataTypes.FloatType, true,
						Metadata.empty()),
				new StructField("return_units", DataTypes.IntegerType, true,
						Metadata.empty()),
				new StructField("returns", DataTypes.FloatType, true,
						Metadata.empty()),
				new StructField("demand", DataTypes.IntegerType, true,
						Metadata.empty()) });
		
		// reading training data
		DataFrame df = sqlContext.read().format("com.databricks.spark.csv")
				.option("header", "true").schema(trainDataSchema)
				.load(trainDataSet);
		
		// preprocessing data
		JavaRDD<ReducedRecordsMeanMedian> res = df.javaRDD().map(
				new Function<Row, ReducedRecordsMeanMedian>() {

					@Override
					public ReducedRecordsMeanMedian call(Row row)
							throws Exception {
						ReducedRecordsMeanMedian rec = new ReducedRecordsMeanMedian();
						rec.setClient_id(row.getInt(4));
						rec.setProduct_id(row.getInt(5));
						rec.setDemand(row.getInt(10));
						return rec;
					}
				});
		
		// data mining part
		df = sqlContext.createDataFrame(res, ReducedRecordsMeanMedian.class);
		df.registerTempTable("res");
		DataFrame dfFin = sqlContext
				.sql("select client_id as client_id,product_id as product_id,cast(avg(demand) as INT) as demand from res group by client_id,product_id ");
		dfFin.registerTempTable("tableGrouped");
		
		// test data schema
		StructType testDataSchema = new StructType(new StructField[] {
				new StructField("id", DataTypes.IntegerType, true,
						Metadata.empty()),
				new StructField("week_number", DataTypes.IntegerType, true,
						Metadata.empty()),
				new StructField("sales_depot_id", DataTypes.IntegerType, true,
						Metadata.empty()),
				new StructField("sales_channel_id", DataTypes.IntegerType,
						true, Metadata.empty()),
				new StructField("route_id", DataTypes.IntegerType, true,
						Metadata.empty()),
				new StructField("client_id", DataTypes.IntegerType, true,
						Metadata.empty()),
				new StructField("product_id", DataTypes.IntegerType, true,
						Metadata.empty()) });

		// reading the test data
		DataFrame dfTest = sqlContext.read().format("com.databricks.spark.csv")
				.option("header", "true").schema(testDataSchema)
				.load(testDataSet);
		dfTest.registerTempTable("tableTest");
		
		// joining data
		DataFrame fdJoined = sqlContext
				.sql("select tableTest.id as id, tableTest.client_id as client_id"
						+ ",tableTest.product_id as product_id, tableGrouped.demand as demand from tableTest left join tableGrouped "
						+ "on tableTest.client_id = tableGrouped.client_id and tableTest.product_id = tableGrouped.product_id order by tableTest.id ");
		fdJoined.show();

		// creating final data frame
		JavaRDD<TestFileFinalStructure> finRdd = fdJoined.javaRDD().map(
				new Function<Row, TestFileFinalStructure>() {

					@Override
					public TestFileFinalStructure call(Row row)
							throws Exception {
						TestFileFinalStructure fin = new TestFileFinalStructure();
						fin.setId(row.getInt(0));
						int demand = 7;
						if (!row.isNullAt(3))
							demand = row.getInt(3);
						fin.setDemanda_uni_equil(demand);
						return fin;
					}
				});
		DataFrame finDF = sqlContext.createDataFrame(finRdd,
				TestFileFinalStructure.class);
		
		// re arranging columns
		finDF = finDF.select("id", "demanda_uni_equil");
		
		// display and get output
		finDF.show();
		finDF.coalesce(1).write().option("header", "true")
				.format("com.databricks.spark.csv").save("finalAvg.csv");

	}
}
