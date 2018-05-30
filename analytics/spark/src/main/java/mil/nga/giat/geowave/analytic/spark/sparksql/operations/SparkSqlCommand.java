package mil.nga.giat.geowave.analytic.spark.sparksql.operations;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
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
import mil.nga.giat.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.cli.remote.options.StoreLoader;

@GeowaveOperation(name = "sql", parentOperation = AnalyticSection.class)
@Parameters(commandDescription = "SparkSQL queries")
public class SparkSqlCommand extends
		ServiceEnabledCommand<Void>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(SparkSqlCommand.class);
	private final static String STORE_ADAPTER_DELIM = "|";
	private final static String CMD_DESCR = "<sql query> - e.g. 'select * from %storename[" + STORE_ADAPTER_DELIM
			+ "adaptername" + STORE_ADAPTER_DELIM + "viewName] where condition...'";

	@Parameter(description = CMD_DESCR)
	private List<String> parameters = new ArrayList<String>();

	@ParametersDelegate
	private SparkSqlOptions sparkSqlOptions = new SparkSqlOptions();

	private DataStorePluginOptions outputDataStore = null;
	private SqlQueryRunner sqlRunner = new SqlQueryRunner();

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

		LOGGER.debug("Input SQL: " + sql);
		String cleanSql = initStores(
				configFile,
				sql,
				sparkSqlOptions.getOutputStoreName());

		LOGGER.debug("Running with cleaned SQL: " + cleanSql);
		sqlRunner.setSql(cleanSql);

		stopwatch.reset();
		stopwatch.start();

		// Execute the query
		Dataset<Row> results = sqlRunner.run();

		stopwatch.stop();

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Spark SQL query took " + stopwatch.getTimeString());
			LOGGER.debug("   and got " + results.count() + " results");
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
		Pattern storeDetect = Pattern.compile("(\\\"[^\\\"]*\\\"|'[^']*')|([%][^.,\\s]+)");
		String escapedDelimRegex = java.util.regex.Pattern.quote(STORE_ADAPTER_DELIM);

		Matcher matchedStore = getFirstPositiveMatcher(
				storeDetect,
				sql);
		String replacedSQL = sql;

		while (matchedStore != null) {
			String parseStore = matchedStore.group(2);
			String originalStoreText = parseStore;

			// Drop the first character off string should be % sign
			parseStore = parseStore.substring(1);
			parseStore = parseStore.trim();

			LOGGER.debug("parsed store: " + parseStore);

			String[] storeNameParts = parseStore.split(escapedDelimRegex);
			LOGGER.debug("Split Count: " + storeNameParts.length);
			for (String split : storeNameParts) {
				LOGGER.debug("Store split: " + split);
			}
			String storeName = null;
			String adapterName = null;
			String viewName = null;
			switch (storeNameParts.length) {
				case 3:
					viewName = storeNameParts[2].trim();
				case 2:
					adapterName = storeNameParts[1].trim();
				case 1:
					storeName = storeNameParts[0].trim();
					break;
				default:
					throw new ParameterException(
							"Ambiguous datastore" + STORE_ADAPTER_DELIM + "adapter designation: " + storeNameParts);
			}

			final StoreLoader inputStoreLoader = new StoreLoader(
					storeName);
			if (!inputStoreLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find input store: " + inputStoreLoader.getStoreName());
			}
			DataStorePluginOptions storeOptions = inputStoreLoader.getDataStorePlugin();

			ByteArrayId adapterId = null;
			if (adapterName != null) {
				adapterId = new ByteArrayId(
						adapterName);
			}
			viewName = sqlRunner.addInputStore(
					storeOptions,
					adapterId,
					viewName);
			if (viewName != null) {
				replacedSQL = StringUtils.replace(
						replacedSQL,
						originalStoreText,
						viewName,
						-1);
			}

			matchedStore = getNextPositiveMatcher(matchedStore);
		}

		return replacedSQL;
	}

	private Matcher getFirstPositiveMatcher(
			Pattern compiledPattern,
			String sql ) {
		Matcher returnMatch = compiledPattern.matcher(sql);
		return getNextPositiveMatcher(returnMatch);
	}

	private Matcher getNextPositiveMatcher(
			Matcher lastMatch ) {
		while (lastMatch.find()) {
			if (lastMatch.group(2) != null) {
				return lastMatch;
			}
		}
		return null;
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			final String sql ) {
		parameters = new ArrayList<String>();
		parameters.add(sql);
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
