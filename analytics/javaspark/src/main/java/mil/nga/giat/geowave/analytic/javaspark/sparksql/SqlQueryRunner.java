package mil.nga.giat.geowave.analytic.javaspark.sparksql;

import java.io.IOException;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.analytic.javaspark.GeoWaveRDD;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

public class SqlQueryRunner
{
	private final static Logger LOGGER = LoggerFactory.getLogger(SqlQueryRunner.class);

	private String appName = "SqlQueryRunner";
	private String master = "local[*]";
	private String host = "localhost";

	private SparkSession spark;
	private JavaSparkContext jsc = null;

	private DataStorePluginOptions inputDataStore1 = null;
	private ByteArrayId adapterId1 = null;
	public static final String TEMP1 = "temp1";

	private DataStorePluginOptions inputDataStore2 = null;
	private ByteArrayId adapterId2 = null;
	public static final String TEMP2 = "temp2";

	private String sql = null;

	public SqlQueryRunner() {}

	private void initContext() {
		spark = SparkSession.builder().master(
				master).appName(
				appName).getOrCreate();

		jsc = new JavaSparkContext(
				spark.sparkContext());
	}

	public void closeContext() {
		if (jsc != null) {
			jsc.close();
			jsc = null;

			spark.close();
		}
	}

	public Dataset<Row> run()
			throws IOException {
		initContext();

		// Validate inputs
		if (inputDataStore1 == null) {
			LOGGER.error("You must supply an input datastore!");
			throw new IOException(
					"You must supply an input datastore!");
		}

		QueryOptions queryOptions = null;
		if (adapterId1 != null) {
			// Retrieve the adapters
			CloseableIterator<DataAdapter<?>> adapterIt = inputDataStore1.createAdapterStore().getAdapters();
			DataAdapter adapterForQuery = null;

			while (adapterIt.hasNext()) {
				DataAdapter adapter = adapterIt.next();

				if (adapter.getAdapterId().equals(
						adapterId1)) {
					adapterForQuery = adapter;
					queryOptions = new QueryOptions(
							adapterForQuery);
					break;
				}
			}
		}

		// Load RDD from datastore
		JavaPairRDD<GeoWaveInputKey, SimpleFeature> rdd1 = GeoWaveRDD.rddForSimpleFeatures(
				jsc.sc(),
				inputDataStore1,
				null,
				queryOptions);

		// Create a DataFrame from the Left RDD
		SimpleFeatureDataFrame dataFrame1 = new SimpleFeatureDataFrame(
				spark);

		if (!dataFrame1.init(
				inputDataStore1,
				adapterId1)) {
			LOGGER.error("Failed to initialize dataframe");
			return null;
		}

		LOGGER.debug(dataFrame1.getSchema().json());

		Dataset<Row> dfTemp1 = dataFrame1.getDataFrame(rdd1);

		if (LOGGER.isDebugEnabled()) {
			dfTemp1.show(
					10,
					false);
		}

		dfTemp1.createOrReplaceTempView(TEMP1);

		if (inputDataStore2 != null) {
			queryOptions = null;
			if (adapterId2 != null) {
				// Retrieve the adapters
				CloseableIterator<DataAdapter<?>> adapterIt = inputDataStore2.createAdapterStore().getAdapters();
				DataAdapter adapterForQuery = null;

				while (adapterIt.hasNext()) {
					DataAdapter adapter = adapterIt.next();

					if (adapter.getAdapterId().equals(
							adapterId2)) {
						adapterForQuery = adapter;
						queryOptions = new QueryOptions(
								adapterForQuery);
						break;
					}
				}
			}

			// Load RDD from datastore
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> rdd2 = GeoWaveRDD.rddForSimpleFeatures(
					jsc.sc(),
					inputDataStore2,
					null,
					queryOptions);

			// Create a DataFrame from the Left RDD
			SimpleFeatureDataFrame dataFrame2 = new SimpleFeatureDataFrame(
					spark);

			if (!dataFrame2.init(
					inputDataStore2,
					adapterId2)) {
				LOGGER.error("Failed to initialize dataframe");
				return null;
			}

			LOGGER.debug(dataFrame2.getSchema().json());

			Dataset<Row> dfTemp2 = dataFrame2.getDataFrame(rdd2);

			if (LOGGER.isDebugEnabled()) {
				dfTemp2.show(
						10,
						false);
			}

			dfTemp2.createOrReplaceTempView(TEMP2);
		}

		// Run the query
		Dataset<Row> results = spark.sql(sql);

		return results;
	}

	public String getAdapterName() {
		if (adapterId1 != null) {
			return adapterId1.getString();
		}

		return null;
	}

	public void setAppName(
			String appName ) {
		this.appName = appName;
	}

	public void setMaster(
			String master ) {
		this.master = master;
	}

	public void setHost(
			String host ) {
		this.host = host;
	}

	public void setInputDataStore1(
			DataStorePluginOptions inputDataStore ) {
		this.inputDataStore1 = inputDataStore;
	}

	public void setAdapterId1(
			ByteArrayId adapterId ) {
		this.adapterId1 = adapterId;
	}

	public void setInputDataStore2(
			DataStorePluginOptions inputDataStore ) {
		this.inputDataStore2 = inputDataStore;
	}

	public void setAdapterId2(
			ByteArrayId adapterId ) {
		this.adapterId2 = adapterId;
	}

	public void setSql(
			String sql ) {
		this.sql = sql;
	}
}
