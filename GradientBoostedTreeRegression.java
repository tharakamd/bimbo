import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.GradientBoostedTrees;
import org.apache.spark.mllib.tree.configuration.BoostingStrategy;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class GradientBoostedTreeRegression {
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
		// reading data
		DataFrame df = sqlContext.read().format("com.databricks.spark.csv")
				.option("header", "true").schema(trainDataSchema)
				.load(trainDataSet);
		// generating parameters
		/*
		 * long d = df.select("week_number" ).count(); System.out.println(d);
		 */
		// mapping data
		JavaRDD<ReducedTrainRecord> res = df.javaRDD().map(
				new Function<Row, ReducedTrainRecord>() {
					@Override
					public ReducedTrainRecord call(Row row) throws Exception {
						ReducedTrainRecord rec = new ReducedTrainRecord();
						rec.setSales_depot_id(row.getInt(1));
						rec.setClient_id(row.getInt(4));
						rec.setProduct_id(row.getInt(5));
						rec.setSales_units(row.getInt(6));
						rec.setReturn_units(row.getInt(8));
						rec.setDemand(row.getInt(10));
						return rec;
					}
				});
		df = sqlContext.createDataFrame(res, ReducedRecordsTMP.class);
		JavaRDD<LabeledPoint> resRow  = res.map(new Function<ReducedRecordsTMP, LabeledPoint>() {
			@Override
			public LabeledPoint call(ReducedRecordsTMP record) throws Exception {
				return null;
			}
			
		});
		// gradient boosted tree
		BoostingStrategy boostingStrategy = BoostingStrategy.defaultParams("Regression");
		boostingStrategy.setNumIterations(3); // Note: Use more iterations in practice.
		boostingStrategy.getTreeStrategy().setMaxDepth(5);
		// Empty categoricalFeaturesInfo indicates all features are continuous.
		Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
		boostingStrategy.treeStrategy().setCategoricalFeaturesInfo(categoricalFeaturesInfo);
		final GradientBoostedTreesModel model =
				  GradientBoostedTrees.train(res, boostingStrategy);
		// output
		df.printSchema();
		df.show();

	}
}
