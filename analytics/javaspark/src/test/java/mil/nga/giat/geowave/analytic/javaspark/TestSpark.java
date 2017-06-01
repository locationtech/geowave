package mil.nga.giat.geowave.analytic.javaspark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class TestSpark
{
	public static void main(
			String[] args ) {
		SparkConf sparkConf = new SparkConf();

		sparkConf.setAppName("Hello Spark");
		sparkConf.setMaster("local");

		JavaSparkContext context = new JavaSparkContext(
				sparkConf);

		mapAndFilter(context);

		reduceAndGroupByKey(context);

		join(context);

		coGroup(context);

		unionAndDistinct(context);

		collect(context);

		count(context);

		reduce(context);

		take(context);

		context.close();
	}

	/**
	 * OUTPUT: [1, 4, 9] [4] [1, 2, 3, 2, 4, 6, 3, 6, 9]
	 * 
	 * @param context
	 */
	private static void mapAndFilter(
			JavaSparkContext context ) {
		System.out.println(">>> Map And Filter");
		
		JavaRDD<Integer> numbersRDD = context.parallelize(
				Arrays.asList(
						1,
						2,
						3));

		JavaRDD<Integer> squaresRDD = numbersRDD.map(
				n -> n * n);
		System.out.println(
				squaresRDD.collect().toString());

		JavaRDD<Integer> evenRDD = squaresRDD.filter(
				n -> n % 2 == 0);
		System.out.println(
				evenRDD.collect().toString());

		JavaRDD<Integer> multipliedRDD = numbersRDD.flatMap(
				n -> Arrays.asList(
						n,
						n * 2,
						n * 3).iterator());
		System.out.println(
				multipliedRDD.collect().toString());
	}

	/**
	 * OUTPUT: [(cat,1), (dog,5), (cat,3)] [(dog,5), (cat,3)] [(dog,[5]),
	 * (cat,[1, 3])]
	 * 
	 * @param context
	 */
	private static void reduceAndGroupByKey(
			JavaSparkContext context ) {
		System.out.println(">>> Reduce And Group By Key");
		
		JavaPairRDD<String, Integer> petsRDD = JavaPairRDD.fromJavaRDD(
				context.parallelize(
						Arrays.asList(
								new Tuple2<String, Integer>(
										"cat",
										1),
								new Tuple2<String, Integer>(
										"dog",
										5),
								new Tuple2<String, Integer>(
										"cat",
										3))));

		System.out.println(
				petsRDD.collect().toString());

		JavaPairRDD<String, Integer> agedPetsRDD = petsRDD.reduceByKey(
				(
						v1,
						v2 ) -> Math.max(
								v1,
								v2));
		System.out.println(
				agedPetsRDD.collect().toString());

		JavaPairRDD<String, Iterable<Integer>> groupedPetsRDD = petsRDD.groupByKey();
		System.out.println(
				groupedPetsRDD.collect().toString());

	}

	/**
	 * OUTPUT: [(index.html,1.2.3.4), (about.html,3.4.5.6),
	 * (index.html,1.3.3.1)] [(index.html,Home), (about.html,About)]
	 * [(index.html,(1.2.3.4,Home)), (index.html,(1.3.3.1,Home)),
	 * (about.html,(3.4.5.6,About))]
	 * 
	 * @param context
	 */
	private static void join(
			JavaSparkContext context ) {
		System.out.println(">>> Join");

		JavaPairRDD<String, String> visitsRDD = JavaPairRDD.fromJavaRDD(context.parallelize(Arrays.asList(
				new Tuple2<String, String>(
						"index.html",
						"1.2.3.4"),
				new Tuple2<String, String>(
						"about.html",
						"3.4.5.6"),
				new Tuple2<String, String>(
						"index.html",
						"1.3.3.1"))));

		System.out.println(visitsRDD.collect().toString());

		JavaPairRDD<String, String> pageNamesRDD = JavaPairRDD.fromJavaRDD(context.parallelize(Arrays.asList(
				new Tuple2<String, String>(
						"index.html",
						"Home"),
				new Tuple2<String, String>(
						"about.html",
						"About"))));

		System.out.println(pageNamesRDD.collect().toString());

		JavaPairRDD<String, Tuple2<String, String>> joinRDD = visitsRDD.join(pageNamesRDD);
		System.out.println(joinRDD.collect().toString());

	}

	/**
	 * OUTPUT: [(index.html,1.2.3.4), (about.html,3.4.5.6),
	 * (index.html,1.3.3.1)] [(index.html,Home), (index.html,Welcome),
	 * (about.html,About)]
	 * 
	 * @param context
	 */
	private static void coGroup(
			JavaSparkContext context ) {
		System.out.println(">>> CoGroup");

		JavaPairRDD<String, String> visitsRDD = JavaPairRDD.fromJavaRDD(context.parallelize(Arrays.asList(
				new Tuple2<String, String>(
						"index.html",
						"1.2.3.4"),
				new Tuple2<String, String>(
						"about.html",
						"3.4.5.6"),
				new Tuple2<String, String>(
						"index.html",
						"1.3.3.1"))));

		System.out.println(visitsRDD.collect().toString());

		JavaPairRDD<String, String> pageNamesRDD = JavaPairRDD.fromJavaRDD(context.parallelize(Arrays.asList(
				new Tuple2<String, String>(
						"index.html",
						"Home"),
				new Tuple2<String, String>(
						"index.html",
						"Welcome"),
				new Tuple2<String, String>(
						"about.html",
						"About"))));

		System.out.println(pageNamesRDD.collect().toString());

		JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> joinRDD = visitsRDD.cogroup(pageNamesRDD);
		System.out.println(joinRDD.collect().toString());
	}

	/**
	 * OUTPUT: [0, 5, 3, 8, 9, 3] [0, 8, 3, 9, 5]
	 * 
	 * @param context
	 */
	private static void unionAndDistinct(
			JavaSparkContext context ) {
		System.out.println(">>> Union And Distinct");

		JavaRDD<Integer> numbers1RDD = context.parallelize(Arrays.asList(
				0,
				5,
				3));
		JavaRDD<Integer> numbers2RDD = context.parallelize(Arrays.asList(
				8,
				9,
				3));

		JavaRDD<Integer> unionRDD = numbers1RDD.union(numbers2RDD);
		System.out.println(unionRDD.collect().toString());

		JavaRDD<Integer> distinctRDD = unionRDD.distinct();
		System.out.println(distinctRDD.collect().toString());
	}

	/**
	 * OUTPUT: [0, 5, 3]
	 * 
	 * @param context
	 */
	private static void collect(
			JavaSparkContext context ) {
		System.out.println(">>> Collect");

		JavaRDD<Integer> numbersRDD = context.parallelize(Arrays.asList(
				0,
				5,
				3));

		List<Integer> numbersList = numbersRDD.collect();
		System.out.println(numbersList.toString());
	}

	/**
	 * OUTPUT: 6
	 * 
	 * @param context
	 */
	private static void count(
			JavaSparkContext context ) {
		System.out.println(">>> Count");

		JavaRDD<Integer> numbersRDD = context.parallelize(Arrays.asList(
				8,
				0,
				5,
				3,
				10,
				6));

		long numbersRDDSize = numbersRDD.count();
		System.out.println(numbersRDDSize);
	}

	/**
	 * OUTPUT: 32
	 * 
	 * @param context
	 */
	private static void reduce(
			JavaSparkContext context ) {
		System.out.println(">>> Reduce");
		
		JavaRDD<Integer> numbersRDD = context.parallelize(
				Arrays.asList(
						8,
						0,
						5,
						3,
						10,
						6));

		long total = numbersRDD.reduce(
				(
						n1,
						n2 ) -> n1 + n2);
		System.out.println(
				total);

	}

	/**
	 * OUTPUT: [8, 0, 5]
	 * 
	 * @param context
	 */
	private static void take(
			JavaSparkContext context ) {
		System.out.println(">>> Take");

		JavaRDD<Integer> numbersRDD = context.parallelize(Arrays.asList(
				8,
				0,
				5,
				3,
				10,
				6));

		List<Integer> numbersList = numbersRDD.take(3);
		System.out.println(numbersList.toString());

	}
}
