package mil.nga.giat.geowave.mapreduce.input;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.mapreduce.JobContextAdapterStore;
import mil.nga.giat.geowave.mapreduce.JobContextIndexStore;
import mil.nga.giat.geowave.mapreduce.MapReduceDataStore;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputConfigurator.InputConfig;

public class GeoWaveInputFormat<T> extends
		InputFormat<GeoWaveInputKey, T>
{
	private static final Class<?> CLASS = GeoWaveInputFormat.class;
	protected static final Logger LOGGER = LoggerFactory.getLogger(CLASS);

	public static void setStoreOptionsMap(
			final Configuration config,
			final Map<String, String> storeConfigOptions ) {
		GeoWaveConfiguratorBase.setStoreOptionsMap(
				CLASS,
				config,
				storeConfigOptions);
	}

	public static void setStoreOptions(
			final Configuration config,
			final DataStorePluginOptions storeOptions ) {
		if (storeOptions != null) {
			GeoWaveConfiguratorBase.setStoreOptionsMap(
					CLASS,
					config,
					storeOptions.getOptionsAsMap());
		}
		else {
			GeoWaveConfiguratorBase.setStoreOptionsMap(
					CLASS,
					config,
					null);
		}
	}

	public static IndexStore getJobContextIndexStore(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getJobContextIndexStore(
				CLASS,
				context);
	}

	public static AdapterIndexMappingStore getJobContextAdapterIndexMappingStore(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getJobContextAdapterIndexMappingStore(
				CLASS,
				context);
	}

	public static AdapterStore getJobContextAdapterStore(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getJobContextAdapterStore(
				CLASS,
				context);
	}

	public static DataStatisticsStore getJobContextDataStatisticsStore(
			final JobContext context ) {
		// TODO, this doesn't create a data statistics store wrapping a
		// jobcontext as the name implies, need to either wrap a job context or
		// rename this (for adapter and index store, adapters and indices are
		// stored in the job context rather than multiple processes needing to
		// look it up, this doesn't seem to be happening for stats)
		return GeoWaveConfiguratorBase.getDataStatisticsStore(
				CLASS,
				context);
	}

	public static void setMinimumSplitCount(
			final Configuration config,
			final Integer minSplits ) {
		GeoWaveInputConfigurator.setMinimumSplitCount(
				CLASS,
				config,
				minSplits);
	}

	public static void setMaximumSplitCount(
			final Configuration config,
			final Integer maxSplits ) {
		GeoWaveInputConfigurator.setMaximumSplitCount(
				CLASS,
				config,
				maxSplits);
	}

	public static void setIsOutputWritable(
			final Configuration config,
			final Boolean isOutputWritable ) {
		config.setBoolean(
				GeoWaveConfiguratorBase.enumToConfKey(
						CLASS,
						InputConfig.OUTPUT_WRITABLE),
				isOutputWritable);
	}

	public static void setQuery(
			final Configuration config,
			final DistributableQuery query ) {
		GeoWaveInputConfigurator.setQuery(
				CLASS,
				config,
				query);
	}

	protected static DistributableQuery getQuery(
			final JobContext context ) {
		return GeoWaveInputConfigurator.getQuery(
				CLASS,
				context);
	}

	public static void setQueryOptions(
			final Configuration config,
			final QueryOptions queryOptions ) {
		final PrimaryIndex index = queryOptions.getIndex();
		if (index != null) {
			// make available to the context index store
			JobContextIndexStore.addIndex(
					config,
					index);
		}

		try {
			// THIS SHOULD GO AWAY, and assume the adapters in the Persistent
			// Data Store
			// instead. It will fail, due to the 'null', if the query options
			// does not
			// contain the adapters
			for (final DataAdapter<?> adapter : queryOptions.getAdaptersArray(null)) {
				// Also store for use the mapper and reducers
				JobContextAdapterStore.addDataAdapter(
						config,
						adapter);
			}
		}
		catch (final Exception e) {
			LOGGER
					.warn(
							"Adapter Ids witih adapters are included in the query options.This, the adapter must be accessible from the data store for use by the consumer/Mapper.",
							e);
		}
		GeoWaveInputConfigurator.setQueryOptions(
				CLASS,
				config,
				queryOptions);
	}

	protected static QueryOptions getQueryOptions(
			final JobContext context ) {
		final QueryOptions options = GeoWaveInputConfigurator.getQueryOptions(
				CLASS,
				context);
		return options == null ? new QueryOptions() : options;
	}

	protected static PrimaryIndex getIndex(
			final JobContext context ) {
		return GeoWaveInputConfigurator.getIndex(
				CLASS,
				GeoWaveConfiguratorBase.getConfiguration(context));
	}

	protected static Boolean isOutputWritable(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getConfiguration(
				context).getBoolean(
				GeoWaveConfiguratorBase.enumToConfKey(
						CLASS,
						InputConfig.OUTPUT_WRITABLE),
				false);
	}

	protected static Integer getMinimumSplitCount(
			final JobContext context ) {
		return GeoWaveInputConfigurator.getMinimumSplitCount(
				CLASS,
				context);
	}

	protected static Integer getMaximumSplitCount(
			final JobContext context ) {
		return GeoWaveInputConfigurator.getMaximumSplitCount(
				CLASS,
				context);
	}

	@Override
	public RecordReader<GeoWaveInputKey, T> createRecordReader(
			final InputSplit split,
			final TaskAttemptContext context )
			throws IOException,
			InterruptedException {
		final Map<String, String> configOptions = getStoreOptionsMap(context);
		final DataStore dataStore = GeoWaveStoreFinder.createDataStore(configOptions);
		final AdapterStore adapterStore = getJobContextAdapterStore(context);
		if ((dataStore != null) && (dataStore instanceof MapReduceDataStore)) {
			final QueryOptions queryOptions = getQueryOptions(context);
			final QueryOptions rangeQueryOptions = new QueryOptions(
					queryOptions);
			return (RecordReader<GeoWaveInputKey, T>) ((MapReduceDataStore) dataStore).createRecordReader(
					getQuery(context),
					rangeQueryOptions,
					adapterStore,
					getJobContextDataStatisticsStore(context),
					getJobContextIndexStore(context),
					isOutputWritable(
							context).booleanValue(),
					split);
		}
		LOGGER.error("Data Store does not support map reduce");
		throw new IOException(
				"Data Store does not support map reduce");
	}

	/**
	 * Check whether a configuration is fully configured to be used with an
	 * Accumulo {@link org.apache.hadoop.mapreduce.InputFormat}.
	 *
	 * @param context
	 *            the Hadoop context for the configured job
	 * @throws IOException
	 *             if the context is improperly configured
	 * @since 1.5.0
	 */
	protected static void validateOptions(
			final JobContext context )
			throws IOException {// attempt to get each of the GeoWave
								// stores
								// from the job context
		try {
			final Map<String, String> configOptions = getStoreOptionsMap(context);
			final StoreFactoryFamilySpi factoryFamily = GeoWaveStoreFinder.findStoreFamily(configOptions);
			if (factoryFamily == null) {
				final String msg = "Unable to find GeoWave data store";
				LOGGER.warn(msg);
				throw new IOException(
						msg);
			}
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Error finding GeoWave stores",
					e);
			throw new IOException(
					"Error finding GeoWave stores",
					e);
		}
	}

	public static DataStorePluginOptions getStoreOptions(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getStoreOptions(
				CLASS,
				context);
	}

	public static Map<String, String> getStoreOptionsMap(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getStoreOptionsMap(
				CLASS,
				context);
	}

	@Override
	public List<InputSplit> getSplits(
			final JobContext context )
			throws IOException,
			InterruptedException {
		final Map<String, String> configOptions = getStoreOptionsMap(context);
		final DataStore dataStore = GeoWaveStoreFinder.createDataStore(configOptions);
		final AdapterStore adapterStore = getJobContextAdapterStore(context);
		if ((dataStore != null) && (dataStore instanceof MapReduceDataStore)) {
			final QueryOptions queryOptions = getQueryOptions(context);
			final QueryOptions rangeQueryOptions = new QueryOptions(
					queryOptions);

			return ((MapReduceDataStore) dataStore).getSplits(
					getQuery(context),
					rangeQueryOptions,
					adapterStore,
					getJobContextDataStatisticsStore(context),
					getJobContextIndexStore(context),
					getMinimumSplitCount(context),
					getMaximumSplitCount(context));
		}

		LOGGER.error("Data Store does not support map reduce");
		throw new IOException(
				"Data Store does not support map reduce");
	}
}
