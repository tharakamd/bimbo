import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.network.protocol.Encoders;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Function1;
import scala.collection.TraversableOnce;
import scala.collection.immutable.ListSet;

public class Miner {

	public static void main(String[] args) {
		// file parths
		String filename = "read.txt";
		String csvName = "chunk0.csv";
		String trainDataSet = "train.csv";
		// confugiring spark
		SparkConf conf = new SparkConf().setMaster("local").setAppName(
				"Bimbo miner");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		// header for train data
		StructType trainDataSchema = new StructType(new StructField[]{
				new StructField("week_number",DataTypes.IntegerType,true,Metadata.empty()),
				new StructField("sales_depot_id",DataTypes.IntegerType,true,Metadata.empty()),
				new StructField("sales_channel_id",DataTypes.IntegerType,true,Metadata.empty()),
				new StructField("route_id",DataTypes.IntegerType,true,Metadata.empty()),
				new StructField("client_id",DataTypes.IntegerType,true,Metadata.empty()),
				new StructField("product_id",DataTypes.IntegerType,true,Metadata.empty()),
				new StructField("sales_units",DataTypes.IntegerType,true,Metadata.empty()),
				new StructField("sales",DataTypes.FloatType,true,Metadata.empty()),
				new StructField("return_units",DataTypes.IntegerType,true,Metadata.empty()),
				new StructField("returns",DataTypes.FloatType,true,Metadata.empty()),
				new StructField("demand",DataTypes.IntegerType,true,Metadata.empty())
		});
		//reading data
		DataFrame df = sqlContext.read().format("com.databricks.spark.csv")
				.option("header", "true")
				.schema(trainDataSchema)
				.load(trainDataSet);
		// generating parameters
/*		long d = df.select("week_number"	).count();
		System.out.println(d);
*/		
		// mapping data
		JavaRDD<TrainRecord> res = df.javaRDD().map(new Function<Row, TrainRecord>() {

			@Override
			public TrainRecord call(Row row) throws Exception {
				TrainRecord rec = new TrainRecord();
				rec.setWeek_number(row.getInt(0));
				rec.setSales_depot_id(row.getInt(1));
				rec.setSales_channel_id(row.getInt(2));
				rec.setRoute_id(row.getInt(3));
				rec.setClient_id(row.getInt(4));
				rec.setProduct_id(row.getInt(5));
				rec.setSales_units(row.getInt(6));
				rec.setSales(row.getFloat(7));
				rec.setReturn_units(row.getInt(8));
				rec.setReturns(row.getFloat(9));
				rec.setDemand(row.getInt(10));
				return rec;
			}
		});
		df = sqlContext.createDataFrame(res, TrainRecord.class);
		
		// calculating max and min values
		DataFrame largest_sales_deport_id = df.describe("sales_depot_id");
		largest_sales_deport_id.printSchema();
		largest_sales_deport_id.show();
		/*df.printSchema();
		df.show();*/
		
	}
}
