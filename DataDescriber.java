import java.util.ArrayList;
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

public class DataDescriber {

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
		JavaRDD<ReducedRecordsTMP> res = df.javaRDD().map(new Function<Row, ReducedRecordsTMP>() {

			@Override
			public ReducedRecordsTMP call(Row row) throws Exception {
				ReducedRecordsTMP rec = new ReducedRecordsTMP();
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
		df = sqlContext.createDataFrame(res, ReducedRecordsTMP.class);
		
		// calculating max and min values
		ArrayList<String> properties = new ArrayList<>();
		properties.add("week_number");
		properties.add("sales_depot_id");
		properties.add("sales_channel_id");
		properties.add("route_id");
		properties.add("client_id");
		properties.add("product_id");
		properties.add("sales_units");
		properties.add("sales");
		properties.add("return_units");
		properties.add("returns");
		properties.add("demand");
		for(String property:properties){
			DataFrame propInfo = df.describe(property);
			propInfo.printSchema();
			propInfo.show();
			propInfo.coalesce(1).write().format("com.databricks.spark.csv").save( property + ".csv");
		}
		/*df.printSchema();
		df.show();*/
		
	}
}
