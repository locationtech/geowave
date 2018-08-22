package mil.nga.giat.geowave.analytic.spark.sparksql;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import mil.nga.giat.geowave.adapter.vector.util.FeatureDataUtils;
import mil.nga.giat.geowave.analytic.spark.GeoWaveRDD;
import mil.nga.giat.geowave.analytic.spark.GeoWaveRDDLoader;
import mil.nga.giat.geowave.analytic.spark.GeoWaveSparkConf;
import mil.nga.giat.geowave.analytic.spark.RDDOptions;
import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomFunction;
import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomWithinDistance;
import mil.nga.giat.geowave.analytic.spark.sparksql.udf.UDFRegistrySPI;
import mil.nga.giat.geowave.analytic.spark.sparksql.udf.UDFRegistrySPI.UDFNameAndConstructor;
import mil.nga.giat.geowave.analytic.spark.spatial.SpatialJoinRunner;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.PersistentAdapterStore;
import mil.nga.giat.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

public class SqlQueryRunner
{
	private final static Logger LOGGER = LoggerFactory.getLogger(SqlQueryRunner.class);

	private String appName = "SqlQueryRunner";
	private String master = "yarn";
	private String host = "localhost";

	private SparkSession session;

	private final HashMap<String, InputStoreInfo> inputStores = new HashMap<String, InputStoreInfo>();
	private final List<ExtractedGeomPredicate> extractedPredicates = new ArrayList<ExtractedGeomPredicate>();
	private String sql = null;

	public SqlQueryRunner() {}

	private void initContext() {
		if (session == null) {
			String jar = "";
			try {
				jar = SqlQueryRunner.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
			}
			catch (final URISyntaxException e) {
				LOGGER.error(
						"Unable to set jar location in spark configuration",
						e);
			}

			session = GeoWaveSparkConf.createSessionFromParams(
					appName,
					master,
					host,
					jar);
		}

	}

	public void close() {
		if (session != null) {
			session.close();
			session = null;
		}
	}

	public Dataset<Row> run()
			throws IOException,
			InterruptedException,
			ExecutionException,
			ParseException {
		initContext();
		// Load stores and create views.
		loadStoresAndViews();

		// Create a version of the sql without string literals to check for
		// subquery syntax in sql statement.
		final Pattern stringLit = Pattern.compile("(?:\\'|\\\").*?(?:\\'|\\\")");
		final Matcher m = stringLit.matcher(sql);
		final String cleanedSql = m.replaceAll("");
		LOGGER.debug("cleaned SQL statement: " + cleanedSql);
		// This regex checks for the presence of multiple sql actions being done
		// in one sql statement.
		// Ultimately this is detecting the presence of subqueries within the
		// sql statement
		// which due to the complexity of breaking down we won't support
		// injecting a optimized join into the process
		if (!cleanedSql
				.matches("(?i)^(?=(?:.*(?:\\b(?:INSERT INTO|UPDATE|SELECT|WITH|DELETE|CREATE TABLE|ALTER TABLE|DROP TABLE)\\b)){2})")) {

			// Parse sparks logical plan for query and determine if spatial join
			// is present
			LogicalPlan plan = null;
			plan = session.sessionState().sqlParser().parsePlan(
					sql);
			final JsonParser gsonParser = new JsonParser();
			final JsonElement jElement = gsonParser.parse(plan.prettyJson());
			if (jElement.isJsonArray()) {
				final JsonArray jArray = jElement.getAsJsonArray();
				final int size = jArray.size();
				for (int iObj = 0; iObj < size; iObj++) {
					final JsonElement childElement = jArray.get(iObj);
					if (childElement.isJsonObject()) {
						final JsonObject jObj = childElement.getAsJsonObject();
						final String objClass = jObj.get(
								"class").getAsString();
						if (Objects.equals(
								objClass,
								"org.apache.spark.sql.catalyst.plans.logical.Filter")) {
							// Search through filter Object to determine if
							// GeomPredicate function present in condition.
							final JsonElement conditionElements = jObj.get("condition");
							if (conditionElements.isJsonArray()) {
								final JsonArray conditionArray = conditionElements.getAsJsonArray();
								final int condSize = conditionArray.size();
								for (int iCond = 0; iCond < condSize; iCond++) {
									final JsonElement childCond = conditionArray.get(iCond);
									if (childCond.isJsonObject()) {
										final JsonObject condObj = childCond.getAsJsonObject();
										final String condClass = condObj.get(
												"class").getAsString();
										if (Objects.equals(
												condClass,
												"org.apache.spark.sql.catalyst.analysis.UnresolvedFunction")) {
											final String udfName = condObj.get(
													"name").getAsJsonObject().get(
													"funcName").getAsString();
											final UDFNameAndConstructor geomUDF = UDFRegistrySPI
													.findFunctionByName(udfName);
											if (geomUDF != null) {
												final ExtractedGeomPredicate relevantPredicate = new ExtractedGeomPredicate();
												relevantPredicate.predicate = geomUDF.getPredicateConstructor().get();
												relevantPredicate.predicateName = udfName;
												extractedPredicates.add(relevantPredicate);
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}

		// We only need to do all this query work if we find a predicate that
		// would indicate a spatial join
		if (extractedPredicates.size() == 1) {
			// This pattern detects the word where outside of quoted areas and
			// captures it in group 2
			final Pattern whereDetect = Pattern.compile("(?i)(\"[^\"]*\"|'[^']*')|(\\bWHERE\\b)");
			final Pattern andOrDetect = Pattern.compile("(?i)(\"[^\"]*\"|'[^']*')|(\\bAND|OR\\b)");
			final Pattern orderGroupDetect = Pattern.compile("(?i)(\"[^\"]*\"|'[^']*')|(\\bORDER BY|GROUP BY\\b)");
			final Matcher filterStart = getFirstPositiveMatcher(
					whereDetect,
					sql);
			if (filterStart == null) {
				LOGGER.error("There should be a where clause matching the pattern. Running default SQL");
				return runDefaultSQL();
			}
			final int whereStart = filterStart.start(2);
			int whereEnd = sql.length();
			final Matcher filterEnd = getFirstPositiveMatcher(
					orderGroupDetect,
					sql.substring(whereStart));
			if (filterEnd != null) {
				whereEnd = filterEnd.start(2);
			}
			final String filterClause = sql.substring(
					whereStart,
					whereEnd);
			LOGGER.warn("Extracted Filter Clause: " + filterClause);

			final Matcher compoundFilter = getFirstPositiveMatcher(
					andOrDetect,
					filterClause);
			if (compoundFilter != null) {
				LOGGER
						.warn("Compound conditional detected can result in multiple joins. Too complex to plan in current context. Running default sql");
				return runDefaultSQL();
			}

			final ExtractedGeomPredicate pred = extractedPredicates.get(0);
			// Parse filter string for predicate location
			final int functionPos = filterClause.indexOf(pred.predicateName);
			final int funcArgStart = filterClause.indexOf(
					"(",
					functionPos);
			final int funcArgEnd = filterClause.indexOf(
					")",
					funcArgStart);
			String funcArgs = filterClause.substring(
					funcArgStart + 1,
					funcArgEnd);
			funcArgs = funcArgs.replaceAll(
					"\\s",
					"");
			LOGGER.warn("Function Args: " + funcArgs);
			final String[] args = funcArgs.split(Pattern.quote(","));
			if (args.length == 2) {
				// Determine valid table relations that map to input stores
				final String[] tableRelations = getTableRelations(args);
				pred.leftTableRelation = tableRelations[0];
				pred.rightTableRelation = tableRelations[1];
			}

			if ((pred.leftTableRelation == null) || (pred.rightTableRelation == null)) {
				LOGGER.warn("Cannot translate table identifier to geowave rdd for join.");
				return runDefaultSQL();
			}

			// Extract radius for distance join from condition
			boolean negativePredicate = false;
			if (Objects.equals(
					pred.predicateName,
					"GeomDistance")) {
				// Look ahead two tokens for logical operand and scalar|boolean
				final String afterFunc = filterClause.substring(funcArgEnd + 1);
				final String[] tokens = afterFunc.split(" ");

				double radius = 0.0;
				if (tokens.length < 2) {
					LOGGER.warn("Could not extract radius for distance join. Running default SQL");
					return runDefaultSQL();
				}
				else {

					final String logicalOperand = tokens[0].trim();
					if ((logicalOperand == ">") || (logicalOperand == ">=")) {
						negativePredicate = true;
					}
					final String radiusStr = tokens[1].trim();
					if (!org.apache.commons.lang3.math.NumberUtils.isNumber(radiusStr)) {
						LOGGER.warn("Could not extract radius for distance join. Running default SQL");
						return runDefaultSQL();
					}
					else {
						final Double r = org.apache.commons.lang3.math.NumberUtils.createDouble(radiusStr);
						if (r == null) {
							LOGGER.warn("Could not extract radius for distance join. Running default SQL");
							return runDefaultSQL();
						}
						radius = r.doubleValue();
					}
				}
				((GeomWithinDistance) pred.predicate).setRadius(radius);
			}
			// At this point we are performing a join
			final SpatialJoinRunner joinRunner = new SpatialJoinRunner(
					session);
			// Collect input store info for join
			final InputStoreInfo leftStore = inputStores.get(pred.leftTableRelation);
			final InputStoreInfo rightStore = inputStores.get(pred.rightTableRelation);

			joinRunner.setNegativeTest(negativePredicate);

			// Setup store info for runner
			final PrimaryIndex[] leftIndices = leftStore.getOrCreateAdapterIndexMappingStore().getIndicesForAdapter(
					leftStore.getOrCreateInternalAdapterStore().getInternalAdapterId(
							leftStore.adapterId)).getIndices(
					leftStore.getOrCreateIndexStore());
			final PrimaryIndex[] rightIndices = rightStore.getOrCreateAdapterIndexMappingStore().getIndicesForAdapter(
					rightStore.getOrCreateInternalAdapterStore().getInternalAdapterId(
							rightStore.adapterId)).getIndices(
					rightStore.getOrCreateIndexStore());
			;
			NumericIndexStrategy leftStrat = null;
			if (leftIndices.length > 0) {
				leftStrat = leftIndices[0].getIndexStrategy();
			}
			NumericIndexStrategy rightStrat = null;
			if (rightIndices.length > 0) {
				rightStrat = rightIndices[0].getIndexStrategy();
			}
			joinRunner.setLeftRDD(GeoWaveRDDLoader.loadIndexedRDD(
					session.sparkContext(),
					leftStore.rdd,
					leftStrat));
			joinRunner.setRightRDD(GeoWaveRDDLoader.loadIndexedRDD(
					session.sparkContext(),
					rightStore.rdd,
					rightStrat));

			joinRunner.setPredicate(pred.predicate);

			joinRunner.setLeftStore(leftStore.storeOptions);
			joinRunner.setRightStore(rightStore.storeOptions);

			// Execute the join
			joinRunner.run();

			// Load results into dataframes and replace original views with
			// joined views
			final SimpleFeatureDataFrame leftResultFrame = new SimpleFeatureDataFrame(
					session);
			final SimpleFeatureDataFrame rightResultFrame = new SimpleFeatureDataFrame(
					session);

			leftResultFrame.init(
					leftStore.storeOptions,
					leftStore.adapterId);
			rightResultFrame.init(
					rightStore.storeOptions,
					rightStore.adapterId);

			final Dataset<Row> leftFrame = leftResultFrame.getDataFrame(joinRunner.getLeftResults());
			final Dataset<Row> rightFrame = rightResultFrame.getDataFrame(joinRunner.getRightResults());
			leftFrame.createOrReplaceTempView(leftStore.viewName);
			rightFrame.createOrReplaceTempView(rightStore.viewName);
		}

		// Run the remaining query through the session sql runner.
		// This will likely attempt to regenerate the join, but should reuse the
		// pairs generated from optimized join beforehand
		final Dataset<Row> results = session.sql(sql);

		return results;
	}

	private Dataset<Row> runDefaultSQL() {
		return session.sql(sql);
	}

	private Matcher getFirstPositiveMatcher(
			final Pattern compiledPattern,
			final String sql ) {
		final Matcher returnMatch = compiledPattern.matcher(sql);
		return getNextPositiveMatcher(returnMatch);
	}

	private Matcher getNextPositiveMatcher(
			final Matcher lastMatch ) {
		while (lastMatch.find()) {
			if (lastMatch.group(2) != null) {
				return lastMatch;
			}
		}
		return null;
	}

	private String[] getTableRelations(
			final String[] predicateArgs ) {
		final String[] outputRelations = {
			getTableNameFromArg(predicateArgs[0].trim()),
			getTableNameFromArg(predicateArgs[1].trim())
		};
		return outputRelations;
	}

	private String getTableNameFromArg(
			final String funcArg ) {
		final String[] attribSplit = funcArg.split(Pattern.quote("."));
		// If we split into two parts the first part will be the relation name
		if (attribSplit.length == 2) {
			final InputStoreInfo storeInfo = inputStores.get(attribSplit[0].trim());
			if (storeInfo != null) {
				return storeInfo.viewName;
			}
		}
		return null;
	}

	private void loadStoresAndViews()
			throws IOException {
		final Collection<InputStoreInfo> addStores = inputStores.values();

		for (final InputStoreInfo storeInfo : addStores) {

			final DataAdapter<?> adapter = storeInfo.getOrCreateAdapterStore().getAdapter(
					storeInfo.getOrCreateInternalAdapterStore().getInternalAdapterId(
							storeInfo.adapterId));
			final QueryOptions queryOptions = new QueryOptions(
					adapter);

			final RDDOptions rddOpts = new RDDOptions();
			rddOpts.setQueryOptions(queryOptions);
			storeInfo.rdd = GeoWaveRDDLoader.loadRDD(
					session.sparkContext(),
					storeInfo.storeOptions,
					rddOpts);

			// Create a DataFrame from the Left RDD
			final SimpleFeatureDataFrame dataFrame = new SimpleFeatureDataFrame(
					session);

			if (!dataFrame.init(
					storeInfo.storeOptions,
					storeInfo.adapterId)) {
				LOGGER.error("Failed to initialize dataframe");
				return;
			}

			LOGGER.debug(dataFrame.getSchema().json());

			final Dataset<Row> dfTemp = dataFrame.getDataFrame(storeInfo.rdd);
			dfTemp.createOrReplaceTempView(storeInfo.viewName);
		}
	}

	public String addInputStore(
			final DataStorePluginOptions storeOptions,
			final ByteArrayId adapterId,
			final String viewName ) {
		if (storeOptions == null) {
			LOGGER.error("Must supply datastore plugin options.");
			return null;
		}
		// If view name is null we will attempt to use adapterId as viewName
		ByteArrayId addAdapter = adapterId;
		// If adapterId is null we grab first adapter available from store
		if (addAdapter == null) {
			final List<ByteArrayId> adapterIds = FeatureDataUtils.getFeatureAdapterIds(storeOptions);
			final int adapterCount = adapterIds.size();
			if (adapterCount > 0) {
				addAdapter = adapterIds.get(0);
			}
			else {
				LOGGER.error("Feature adapter not found in store. One must be specified manually");
				return null;
			}
		}
		String addView = viewName;
		if (addView == null) {
			addView = addAdapter.getString();
		}
		// Check if store exists already using that view name
		if (inputStores.containsKey(addView)) {
			return addView;
		}
		// Create and add new store info if we make it to this point
		final InputStoreInfo inputInfo = new InputStoreInfo(
				storeOptions,
				addAdapter,
				addView);
		inputStores.put(
				addView,
				inputInfo);
		return addView;
	}

	public void removeInputStore(
			final String viewName ) {
		inputStores.remove(viewName);
	}

	public void removeAllStores() {
		inputStores.clear();
	}

	public void setSparkSession(
			final SparkSession session ) {
		this.session = session;
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

	public void setSql(
			final String sql ) {
		this.sql = sql;
	}

	private class InputStoreInfo
	{
		public InputStoreInfo(
				final DataStorePluginOptions storeOptions,
				final ByteArrayId adapterId,
				final String viewName ) {
			this.storeOptions = storeOptions;
			this.adapterId = adapterId;
			this.viewName = viewName;
		}

		private final DataStorePluginOptions storeOptions;
		private IndexStore indexStore = null;
		private PersistentAdapterStore adapterStore = null;
		private InternalAdapterStore internalAdapterStore = null;
		private AdapterIndexMappingStore adapterIndexMappingStore = null;
		private final ByteArrayId adapterId;
		private final String viewName;
		private GeoWaveRDD rdd = null;

		private IndexStore getOrCreateIndexStore() {
			if (indexStore == null) {
				indexStore = storeOptions.createIndexStore();
			}
			return indexStore;
		}

		private PersistentAdapterStore getOrCreateAdapterStore() {
			if (adapterStore == null) {
				adapterStore = storeOptions.createAdapterStore();
			}
			return adapterStore;

		}

		private InternalAdapterStore getOrCreateInternalAdapterStore() {
			if (internalAdapterStore == null) {
				internalAdapterStore = storeOptions.createInternalAdapterStore();
			}
			return internalAdapterStore;
		}

		private AdapterIndexMappingStore getOrCreateAdapterIndexMappingStore() {
			if (adapterIndexMappingStore == null) {
				adapterIndexMappingStore = storeOptions.createAdapterIndexMappingStore();
			}
			return adapterIndexMappingStore;
		}
	}

	private class ExtractedGeomPredicate
	{
		private GeomFunction predicate;
		private String predicateName;
		private String leftTableRelation = null;
		private String rightTableRelation = null;
	}
}
