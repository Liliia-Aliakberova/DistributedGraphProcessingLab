package exercise_4;

import com.clearspring.analytics.util.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Graph;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.graphx.GraphLoader;

public class Exercise_4 {
	
	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) {


		JavaRDD<String> csv = ctx.textFile("src\\main\\resources\\wiki-vertices.txt");

		JavaRDD<Row>   vertices_rdd = csv.map( line -> {
				String[] fields = line.split("\t");
				return  RowFactory.create(fields[0].trim(), fields[1].trim());
			});

		StructType schema = new StructType(new StructField[]{
				new StructField("id", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("value", DataTypes.StringType, true, new MetadataBuilder().build())});

		Dataset<Row>  vertices = sqlCtx.createDataFrame(vertices_rdd, schema);


		JavaRDD<String> csv_edges = ctx.textFile("src\\main\\resources\\wiki-edges.txt");

		JavaRDD<Row>   edges_rdd = csv_edges.map( line -> {
			String[] fields = line.split("\t");
			return  RowFactory.create(fields[0].trim(),fields[1].trim());
		});

		StructType schema_edges = new StructType(new StructField[]{
				new StructField("src",DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("dst", DataTypes.StringType, true, new MetadataBuilder().build())});

		Dataset<Row>  edges = sqlCtx.createDataFrame(edges_rdd, schema_edges);

		System.out.println(vertices);
		System.out.println(edges);

		GraphFrame gf = GraphFrame.apply(vertices,edges);

		System.out.println(gf);


		gf.pageRank();


		gf.edges().show();
		gf.vertices().show();


		GraphFrame gf_rank = gf.pageRank().resetProbability(0.4).maxIter(10).run();
		Dataset<Row> topVertices = gf_rank.vertices().sort(org.apache.spark.sql.functions.desc("pagerank"));
		topVertices.show(10);


	}
	
}
