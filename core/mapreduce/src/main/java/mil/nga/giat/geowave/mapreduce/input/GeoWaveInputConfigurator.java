package mil.nga.giat.geowave.mapreduce.input;

import java.io.IOException;

import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.mapreduce.JobContextIndexStore;

/**
 * This class provides utility methods for accessing job context configuration
 * parameters that are specific to the GeoWaveInputFormat.
 */
public class GeoWaveInputConfigurator extends
		GeoWaveConfiguratorBase
{
	protected static enum InputConfig {
		QUERY,
		QUERY_OPTIONS,
		MIN_SPLITS,
		MAX_SPLITS,
		OUTPUT_WRITABLE // used to inform the input format to output a Writable
						// from the HadoopDataAdapter
	}

	private static DistributableQuery getQueryInternal(
			final Class<?> implementingClass,
			final Configuration configuration ) {
		final String queryStr = configuration.get(
				enumToConfKey(
						implementingClass,
						InputConfig.QUERY),
				"");
		if ((queryStr != null) && !queryStr.isEmpty()) {
			final byte[] queryBytes = ByteArrayUtils.byteArrayFromString(queryStr);
			return PersistenceUtils.fromBinary(
					queryBytes,
					DistributableQuery.class);
		}
		return null;
	}

	private static QueryOptions getQueryOptionsInternal(
			final Class<?> implementingClass,
			final Configuration configuration ) {
		final String queryStr = configuration.get(
				enumToConfKey(
						implementingClass,
						InputConfig.QUERY_OPTIONS),
				"");
		if ((queryStr != null) && !queryStr.isEmpty()) {
			final byte[] queryBytes = ByteArrayUtils.byteArrayFromString(queryStr);
			return PersistenceUtils.fromBinary(
					queryBytes,
					QueryOptions.class);
		}
		return null;
	}

	private static Integer getMinimumSplitCountInternal(
			final Class<?> implementingClass,
			final Configuration configuration ) {
		return getIntegerConfigInternal(
				implementingClass,
				configuration,
				InputConfig.MIN_SPLITS);
	}

	private static Integer getMaximumSplitCountInternal(
			final Class<?> implementingClass,
			final Configuration configuration ) {
		return getIntegerConfigInternal(
				implementingClass,
				configuration,
				InputConfig.MAX_SPLITS);
	}

	private static Integer getIntegerConfigInternal(
			final Class<?> implementingClass,
			final Configuration configuration,
			final InputConfig inputConfig ) {
		final String str = configuration.get(
				enumToConfKey(
						implementingClass,
						inputConfig),
				"");
		if ((str != null) && !str.isEmpty()) {
			final Integer retVal = Integer.parseInt(str);
			return retVal;
		}
		return null;
	}

	public static DistributableQuery getQuery(
			final Class<?> implementingClass,
			final JobContext context ) {
		return getQueryInternal(
				implementingClass,
				getConfiguration(context));
	}

	public static void setQuery(
			final Class<?> implementingClass,
			final Configuration config,
			final DistributableQuery query ) {
		if (query != null) {
			config.set(
					enumToConfKey(
							implementingClass,
							InputConfig.QUERY),
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(query)));
		}
		else {
			config.unset(enumToConfKey(
					implementingClass,
					InputConfig.QUERY));
		}
	}

	public static QueryOptions getQueryOptions(
			final Class<?> implementingClass,
			final JobContext context ) {
		return getQueryOptionsInternal(
				implementingClass,
				getConfiguration(context));
	}

	public static void setQueryOptions(
			final Class<?> implementingClass,
			final Configuration config,
			final QueryOptions queryOptions ) {
		if (queryOptions != null) {
			config.set(
					enumToConfKey(
							implementingClass,
							InputConfig.QUERY_OPTIONS),
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(queryOptions)));
		}
		else {
			config.unset(enumToConfKey(
					implementingClass,
					InputConfig.QUERY_OPTIONS));
		}
	}

	public static Integer getMinimumSplitCount(
			final Class<?> implementingClass,
			final JobContext context ) {
		return getMinimumSplitCountInternal(
				implementingClass,
				getConfiguration(context));
	}

	public static void setMinimumSplitCount(
			final Class<?> implementingClass,
			final Configuration config,
			final Integer minSplits ) {
		if (minSplits != null) {
			config.set(
					enumToConfKey(
							implementingClass,
							InputConfig.MIN_SPLITS),
					minSplits.toString());
		}
		else {
			config.unset(enumToConfKey(
					implementingClass,
					InputConfig.MIN_SPLITS));
		}
	}

	public static Integer getMaximumSplitCount(
			final Class<?> implementingClass,
			final JobContext context ) {
		return getMaximumSplitCountInternal(
				implementingClass,
				getConfiguration(context));
	}

	public static void setMaximumSplitCount(
			final Class<?> implementingClass,
			final Configuration config,
			final Integer maxSplits ) {
		if (maxSplits != null) {
			config.set(
					enumToConfKey(
							implementingClass,
							InputConfig.MAX_SPLITS),
					maxSplits.toString());
		}
		else {
			config.unset(enumToConfKey(
					implementingClass,
					InputConfig.MAX_SPLITS));
		}
	}

	public static PrimaryIndex[] searchForIndices(
			final Class<?> implementingClass,
			final JobContext context ) {
		PrimaryIndex[] userIndices = JobContextIndexStore.getIndices(context);
		if ((userIndices == null) || (userIndices.length <= 0)) {
			// if there are no indices, assume we are searching all indices
			// in the metadata store
			try (CloseableIterator<Index<?, ?>> indicesIterator = getIndexStore(
					implementingClass,
					context).getIndices()) {
				userIndices = (PrimaryIndex[]) IteratorUtils.toArray(
						indicesIterator,
						PrimaryIndex.class);
			}
			catch (IOException e) {
				LOGGER.warn(
						"Unable to close CloseableIterator",
						e);
			}
		}
		return userIndices;
	}
}