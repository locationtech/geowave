package mil.nga.giat.geowave.analytic.spark.sparksql.operations;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.vividsolutions.jts.util.Stopwatch;

import mil.nga.giat.geowave.analytic.spark.sparksql.SqlQueryRunner;
import mil.nga.giat.geowave.analytic.spark.sparksql.SqlResultsWriter;
import mil.nga.giat.geowave.analytic.mapreduce.operations.AnalyticSection;
import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;

@GeowaveOperation(name = "sql", parentOperation = AnalyticSection.class)
@Parameters(commandDescription = "SparkSQL queries")
public class SparkSqlCommand extends
		ServiceEnabledCommand<Void>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(SparkSqlCommand.class);
	private final static String STORE_ADAPTER_DELIM = "|";
	private final static String CMD_DESCR = "<sql query> - e.g. 'select * from storename[" + STORE_ADAPTER_DELIM
			+ "adaptername] where condition...'";

	@Parameter(description = CMD_DESCR)
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
		computeResults(params);
	}

	@Override
	public Void computeResults(
			OperationParams params )
			throws Exception {

		// Config file
		final File configFile = getGeoWaveConfigFile(params);

		final String sql = parameters.get(0);

		String cleanSql = initStores(
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

		LOGGER.debug("Running with SQL: " + cleanSql);
		sqlRunner.setSql(cleanSql);

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

		JCommander.getConsole().println(
				"GeoWave SparkSQL query returned " + results.count() + " results");

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

			JCommander.getConsole().println(
					"Writing GeoWave SparkSQL query results to datastore...");
			sqlResultsWriter.writeResults(typeName);
			JCommander.getConsole().println(
					"Datastore write complete.");
		}

		if (sparkSqlOptions.getCsvOutputFile() != null) {
			results.repartition(
					1).write().format(
					"com.databricks.spark.csv").option(
					"header",
					"true").mode(
					SaveMode.Overwrite).save(
					sparkSqlOptions.getCsvOutputFile());

		}
		return null;
	}

	private String initStores(
			File configFile,
			String sql,
			String outputStoreName ) {
		// Extract input store(s) from sql
		String inputStoreInfo1 = null;
		String inputStoreInfo2 = null;
		String escapedSpaceRegex = java.util.regex.Pattern.quote(" ");
		String crapRegex = "[^a-zA-Z\\d\\s]";

		LOGGER.debug("Input SQL: " + sql);

		String[] sqlSplits = sql.split(escapedSpaceRegex);
		int splitIndex = 0;

		StringBuffer cleanSqlBuf = new StringBuffer();

		while (splitIndex < sqlSplits.length) {
			String split = sqlSplits[splitIndex];

			if (split.equalsIgnoreCase("from")) {
				cleanSqlBuf.append(split);
				cleanSqlBuf.append(" ");

				if (splitIndex < sqlSplits.length - 1) {
					splitIndex++;
					inputStoreInfo1 = sqlSplits[splitIndex];
					LOGGER.debug("Input store info (1): " + inputStoreInfo1);

					cleanSqlBuf.append(inputStoreInfo1.replaceAll(
							crapRegex,
							"_"));
				}
			}
			else if (split.equalsIgnoreCase("join")) {
				cleanSqlBuf.append(split);
				cleanSqlBuf.append(" ");

				if (splitIndex < sqlSplits.length - 1) {
					splitIndex++;
					inputStoreInfo2 = sqlSplits[splitIndex];
					LOGGER.debug("Input store info (2): " + inputStoreInfo2);

					cleanSqlBuf.append(inputStoreInfo2.replaceAll(
							crapRegex,
							"_"));
				}
			}
			else {
				cleanSqlBuf.append(split);
			}

			if (splitIndex < sqlSplits.length - 1) {
				cleanSqlBuf.append(" ");
			}

			splitIndex++;
		}

		// For eliminating invalid chars in view names
		String escapedDelimRegex = java.util.regex.Pattern.quote(STORE_ADAPTER_DELIM);

		// Parse SQL for store.adapter
		if (inputStoreInfo1 != null) {
			String inputStoreName = inputStoreInfo1;

			String adapterName = null;

			if (inputStoreInfo1.contains(STORE_ADAPTER_DELIM)) {
				String[] infoParts = inputStoreInfo1.split(escapedDelimRegex);

				if (infoParts.length > 2) {
					throw new ParameterException(
							"Ambiguous datastore" + STORE_ADAPTER_DELIM + "adapter designation: " + inputStoreInfo1);
				}

				inputStoreName = infoParts[0];
				adapterName = infoParts[1];

				inputStoreInfo1 = inputStoreInfo1.replaceAll(
						crapRegex,
						"_");
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
			LOGGER.debug("Temp view (1): " + tempView1);
		}

		// If there's a join, set up the 2nd store info
		if (inputStoreInfo2 != null) {
			String inputStoreName = inputStoreInfo2;
			String adapterName = null;

			if (inputStoreInfo2.contains(STORE_ADAPTER_DELIM)) {
				String[] infoParts = inputStoreInfo2.split(escapedDelimRegex);

				if (infoParts.length > 2) {
					throw new ParameterException(
							"Ambiguous datastore" + STORE_ADAPTER_DELIM + "adapter designation: " + inputStoreInfo2);
				}

				inputStoreName = infoParts[0];
				adapterName = infoParts[1];

				inputStoreInfo2 = inputStoreInfo2.replaceAll(
						crapRegex,
						"_");
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
			LOGGER.debug("Temp view (2): " + tempView2);
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

		// Clean up any other disallowed chars:
		return cleanSqlBuf.toString();
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
