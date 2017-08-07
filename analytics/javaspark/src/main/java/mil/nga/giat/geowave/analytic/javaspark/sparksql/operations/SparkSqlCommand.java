package mil.nga.giat.geowave.analytic.javaspark.sparksql.operations;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.vividsolutions.jts.util.Stopwatch;

import mil.nga.giat.geowave.analytic.javaspark.sparksql.SqlQueryRunner;
import mil.nga.giat.geowave.analytic.javaspark.sparksql.SqlResultsWriter;
import mil.nga.giat.geowave.analytic.mapreduce.operations.AnalyticSection;
import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;

@GeowaveOperation(name = "sql", parentOperation = AnalyticSection.class)
@Parameters(commandDescription = "SparkSQL queries")
public class SparkSqlCommand extends
		DefaultOperation implements
		Command
{
	private final static Logger LOGGER = LoggerFactory.getLogger(SparkSqlCommand.class);
	private final static String STORE_ADAPTER_DELIM = "$"; // can be _ or $

	@Parameter(description = "<sql query> - e.g. 'select * from storename[$adaptername] where condition...'")
	private List<String> parameters = new ArrayList<String>();

	@ParametersDelegate
	private SparkSqlOptions sparkSqlOptions = new SparkSqlOptions();

	private DataStorePluginOptions inputDataStore1 = null;
	private ByteArrayId adapterId1 = null;
	private String tempView1 = null;

	private DataStorePluginOptions inputDataStore2 = null;
	private ByteArrayId adapterId2 = null;
	private String tempView2 = null;

	DataStorePluginOptions outputDataStore = null;

	// Log some timing
	Stopwatch stopwatch = new Stopwatch();

	@Override
	public void execute(
			final OperationParams params )
			throws Exception {
		// Ensure we have all the required arguments
		if (parameters.size() != 1) {
			throw new ParameterException(
					"Requires argument: <sql query>");
		}

		// Config file
		final File configFile = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);

		final String sql = parameters.get(0);

		initStores(
				configFile,
				sql,
				sparkSqlOptions.getOutputStoreName());

		SqlQueryRunner sqlRunner = new SqlQueryRunner();
		sqlRunner.setInputDataStore1(inputDataStore1);
		sqlRunner.setAdapterId1(adapterId1);
		sqlRunner.setTempView1(tempView1);

		if (inputDataStore2 != null) {
			sqlRunner.setInputDataStore2(inputDataStore2);
			sqlRunner.setAdapterId2(adapterId2);
			sqlRunner.setTempView2(tempView2);
		}

		sqlRunner.setSql(sql);

		stopwatch.reset();
		stopwatch.start();

		// Execute the query
		Dataset<Row> results = sqlRunner.run();

		stopwatch.stop();

		LOGGER.debug("Spark SQL query took " + stopwatch.getTimeString());
		LOGGER.debug("   and got " + results.count() + " results");

		if (LOGGER.isDebugEnabled()) {
			results.printSchema();
		}

		if (sparkSqlOptions.getShowResults() > 0) {
			results.show(
					sparkSqlOptions.getShowResults(),
					false);
		}

		System.out.println("GeoWave SparkSQL query returned " + results.count() + " results");

		if (outputDataStore != null) {
			SqlResultsWriter sqlResultsWriter = new SqlResultsWriter(
					results,
					outputDataStore);

			String typeName = sparkSqlOptions.getOutputTypeName();
			if (typeName == null) {
				typeName = sqlRunner.getAdapterName();
			}

			if (typeName == null) {
				typeName = "sqlresults";
			}

			System.out.print("Writing GeoWave SparkSQL query results...");
			sqlResultsWriter.writeResults(typeName);
			System.out.println("Done.");
		}
	}

	private void initStores(
			File configFile,
			String sql,
			String outputStoreName ) {
		// Extract input store(s) from sql
		String inputStoreInfo1 = null;
		String inputStoreInfo2 = null;

		StringTokenizer tokenizer = new StringTokenizer(
				sql,
				" ");
		while (tokenizer.hasMoreTokens()) {
			String token = tokenizer.nextToken();

			if (token.equalsIgnoreCase("from")) {
				if (tokenizer.hasMoreTokens()) {
					inputStoreInfo1 = tokenizer.nextToken();
					LOGGER.debug("Input store info (1): " + inputStoreInfo1);
				}
			}
			else if (token.equalsIgnoreCase("join")) {
				if (tokenizer.hasMoreTokens()) {
					inputStoreInfo2 = tokenizer.nextToken();
					LOGGER.debug("Input store info (2): " + inputStoreInfo2);
				}
			}
		}

		// Parse SQL for store.adapter
		if (inputStoreInfo1 != null) {
			String inputStoreName = inputStoreInfo1;
			String adapterName = null;

			if (inputStoreInfo1.contains(STORE_ADAPTER_DELIM)) {
				String[] infoParts = inputStoreInfo1.split("\\" + STORE_ADAPTER_DELIM);

				if (infoParts.length > 2) {
					throw new ParameterException(
							"Ambiguous datastore_adapter designation: " + inputStoreInfo1);
				}

				inputStoreName = infoParts[0];
				adapterName = infoParts[1];
			}

			LOGGER.debug("Input store (1): " + inputStoreName);
			LOGGER.debug("Input adapter (1): " + adapterName);

			final StoreLoader inputStoreLoader = new StoreLoader(
					inputStoreName);
			if (!inputStoreLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find input store: " + inputStoreLoader.getStoreName());
			}
			inputDataStore1 = inputStoreLoader.getDataStorePlugin();

			if (adapterName != null) {
				adapterId1 = new ByteArrayId(
						adapterName);
			}

			// We use datastore_adapter as the temp view name
			tempView1 = inputStoreInfo1;
		}

		// If there's a join, set up the 2nd store info
		if (inputStoreInfo2 != null) {
			String inputStoreName = inputStoreInfo2;
			String adapterName = null;

			if (inputStoreInfo2.contains(STORE_ADAPTER_DELIM)) {
				String[] infoParts = inputStoreInfo2.split("\\" + STORE_ADAPTER_DELIM);

				if (infoParts.length > 2) {
					throw new ParameterException(
							"Ambiguous datastore_adapter designation: " + inputStoreInfo2);
				}

				inputStoreName = infoParts[0];
				adapterName = infoParts[1];
			}

			LOGGER.debug("Input store (2): " + inputStoreName);
			LOGGER.debug("Input adapter (2): " + adapterName);

			final StoreLoader inputStoreLoader = new StoreLoader(
					inputStoreName);
			if (!inputStoreLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find input store: " + inputStoreLoader.getStoreName());
			}
			inputDataStore2 = inputStoreLoader.getDataStorePlugin();

			if (adapterName != null) {
				adapterId2 = new ByteArrayId(
						adapterName);
			}

			tempView2 = inputStoreInfo2;
		}

		if (outputStoreName != null) {
			final StoreLoader outputStoreLoader = new StoreLoader(
					outputStoreName);
			if (!outputStoreLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find output store: " + outputStoreLoader.getStoreName());
			}
			outputDataStore = outputStoreLoader.getDataStorePlugin();
		}
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			final String sql ) {
		parameters = new ArrayList<String>();
		parameters.add(sql);
	}

	public DataStorePluginOptions getInputStoreOptions() {
		return inputDataStore1;
	}

	public void setInputStoreOptions(
			final DataStorePluginOptions inputStoreOptions ) {
		inputDataStore1 = inputStoreOptions;
	}

	public DataStorePluginOptions getOutputStoreOptions() {
		return outputDataStore;
	}

	public void setOutputStoreOptions(
			final DataStorePluginOptions outputStoreOptions ) {
		outputDataStore = outputStoreOptions;
	}

	public SparkSqlOptions getSparkSqlOptions() {
		return sparkSqlOptions;
	}

	public void setSparkSqlOptions(
			final SparkSqlOptions sparkSqlOptions ) {
		this.sparkSqlOptions = sparkSqlOptions;
	}
}
