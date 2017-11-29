package mil.nga.giat.geowave.analytic.spark.sparksql;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.analytic.spark.GeoWaveRDD;
import mil.nga.giat.geowave.analytic.spark.kmeans.KMeansRunner;
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
	private String master = "yarn";
	private String host = "localhost";

	private SparkSession session;
	private JavaSparkContext jsc = null;

	private DataStorePluginOptions inputDataStore1 = null;
	private ByteArrayId adapterId1 = null;
	private String tempView1 = null;

	private DataStorePluginOptions inputDataStore2 = null;
	private ByteArrayId adapterId2 = null;
	private String tempView2 = null;

	private String sql = null;

	public SqlQueryRunner() {}

	private void initContext() {
		if (jsc == null) {
			String jar = "";
			try {
				jar = KMeansRunner.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
			}
			catch (final URISyntaxException e) {
				LOGGER.error(
						"Unable to set jar location in spark configuration",
						e);
			}
			session = SparkSession.builder().appName(
					appName).master(
					master).config(
					"spark.driver.host",
					host).config(
					"spark.jars",
					jar).getOrCreate();

			jsc = new JavaSparkContext(
					session.sparkContext());
		}
	}

	public void close() {
		if (jsc != null) {
			jsc.close();
			jsc = null;
		}
		if (session != null) {
			session.close();
			session = null;
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
			final CloseableIterator<DataAdapter<?>> adapterIt = inputDataStore1.createAdapterStore().getAdapters();
			DataAdapter adapterForQuery = null;

			while (adapterIt.hasNext()) {
				final DataAdapter adapter = adapterIt.next();

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
		final JavaPairRDD<GeoWaveInputKey, SimpleFeature> rdd1 = GeoWaveRDD.rddForSimpleFeatures(
				jsc.sc(),
				inputDataStore1,
				null,
				queryOptions);

		// Create a DataFrame from the Left RDD
		final SimpleFeatureDataFrame dataFrame1 = new SimpleFeatureDataFrame(
				session);

		if (!dataFrame1.init(
				inputDataStore1,
				adapterId1)) {
			LOGGER.error("Failed to initialize dataframe");
			return null;
		}

		LOGGER.debug(dataFrame1.getSchema().json());

		final Dataset<Row> dfTemp1 = dataFrame1.getDataFrame(rdd1);

		if (LOGGER.isDebugEnabled()) {
			dfTemp1.show(
					10,
					false);
		}

		dfTemp1.createOrReplaceTempView(tempView1);

		if (inputDataStore2 != null) {
			queryOptions = null;
			if (adapterId2 != null) {
				// Retrieve the adapters
				final CloseableIterator<DataAdapter<?>> adapterIt = inputDataStore2.createAdapterStore().getAdapters();
				DataAdapter adapterForQuery = null;

				while (adapterIt.hasNext()) {
					final DataAdapter adapter = adapterIt.next();

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
			final JavaPairRDD<GeoWaveInputKey, SimpleFeature> rdd2 = GeoWaveRDD.rddForSimpleFeatures(
					jsc.sc(),
					inputDataStore2,
					null,
					queryOptions);

			// Create a DataFrame from the Left RDD
			final SimpleFeatureDataFrame dataFrame2 = new SimpleFeatureDataFrame(
					session);

			if (!dataFrame2.init(
					inputDataStore2,
					adapterId2)) {
				LOGGER.error("Failed to initialize dataframe");
				return null;
			}

			LOGGER.debug(dataFrame2.getSchema().json());

			final Dataset<Row> dfTemp2 = dataFrame2.getDataFrame(rdd2);

			if (LOGGER.isDebugEnabled()) {
				dfTemp2.show(
						10,
						false);
			}

			dfTemp2.createOrReplaceTempView(tempView2);
		}

		// Run the query
		final Dataset<Row> results = session.sql(sql);

		return results;
	}

	public String getAdapterName() {
		if (adapterId1 != null) {
			return adapterId1.getString();
		}

		return null;
	}

	public void setAppName(
			final String appName ) {
		this.appName = appName;
	}

	public void setMaster(
			final String master ) {
		this.master = master;
	}

	public void setHost(
			final String host ) {
		this.host = host;
	}

	public void setInputDataStore1(
			final DataStorePluginOptions inputDataStore ) {
		inputDataStore1 = inputDataStore;
	}

	public void setAdapterId1(
			final ByteArrayId adapterId ) {
		adapterId1 = adapterId;
	}

	public void setTempView1(
			final String tempView1 ) {
		this.tempView1 = tempView1;
	}

	public void setInputDataStore2(
			final DataStorePluginOptions inputDataStore ) {
		inputDataStore2 = inputDataStore;
	}

	public void setAdapterId2(
			final ByteArrayId adapterId ) {
		adapterId2 = adapterId;
	}

	public void setTempView2(
			final String tempView2 ) {
		this.tempView2 = tempView2;
	}

	public void setSql(
			final String sql ) {
		this.sql = sql;
	}
}
