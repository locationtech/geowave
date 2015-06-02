/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase.mapreduce.input;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.datastore.hbase.mapreduce.GeoWaveHBaseConfiguratorBase;
import mil.nga.giat.geowave.datastore.hbase.mapreduce.JobContextHBaseAdapterStore;
import mil.nga.giat.geowave.datastore.hbase.mapreduce.JobContextHBaseIndexStore;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * @author viggy Functionality similar to <code> GeoWaveInputFormat </code>
 */
public class GeoWaveHBaseInputFormat<T> extends
		InputFormat<GeoWaveHBaseInputKey, T>
{

	private static final Class<?> CLASS = GeoWaveHBaseInputFormat.class;
	protected static final Logger LOGGER = Logger.getLogger(CLASS);
	private static final BigInteger TWO = BigInteger.valueOf(2L);

	@Override
	public List<InputSplit> getSplits(
			JobContext context )
			throws IOException,
			InterruptedException {
		// TODO #406 Need to fix

		final List<InputSplit> retVal = new ArrayList<InputSplit>();
		LOGGER.error("This method getSplits1 is not yet coded. Need to fix it");
		return retVal;

	}

	@Override
	public RecordReader<GeoWaveHBaseInputKey, T> createRecordReader(
			InputSplit split,
			TaskAttemptContext context )
			throws IOException,
			InterruptedException {
		// TODO #406 Need to fix
		LOGGER.error("This method createRecordReader2 is not yet coded. Need to fix it");
		return null;
	}

	public static void setOperationsInfo(
			Configuration config,
			String zooKeepers,
			String geowaveTableNamespace ) {
		GeoWaveHBaseConfiguratorBase.setOperationsInfo(
				CLASS,
				config,
				zooKeepers,
				geowaveTableNamespace);

	}

	public static void setOperationsInfo(
			final Job job,
			final String zooKeepers,
			final String geowaveTableNamespace ) {
		setOperationsInfo(
				job.getConfiguration(),
				zooKeepers,
				geowaveTableNamespace);
	}

	public static BasicHBaseOperations getOperations(
			final JobContext context )
			throws IOException {
		return GeoWaveHBaseConfiguratorBase.getOperations(
				CLASS,
				context);
	}

	public static void addDataAdapter(
			final Configuration config,
			final DataAdapter<?> adapter ) {

		JobContextHBaseAdapterStore.addDataAdapter(
				config,
				adapter);
		GeoWaveHBaseConfiguratorBase.addDataAdapter(
				CLASS,
				config,
				adapter);
	}

	public static void addIndex(
			final Configuration config,
			final Index index ) {
		JobContextHBaseIndexStore.addIndex(
				config,
				index);
	}

	public static void setQuery(
			final Configuration config,
			final DistributableQuery query ) {
		GeoWaveHBaseInputConfigurator.setQuery(
				CLASS,
				config,
				query);
	}

	public static void setMinimumSplitCount(
			final Configuration config,
			final Integer minSplits ) {
		GeoWaveHBaseInputConfigurator.setMinimumSplitCount(
				CLASS,
				config,
				minSplits);
	}

	public static void setMaximumSplitCount(
			final Configuration config,
			final Integer maxSplits ) {
		GeoWaveHBaseInputConfigurator.setMaximumSplitCount(
				CLASS,
				config,
				maxSplits);
	}

	protected static Level getLogLevel(
			final JobContext context ) {
		// TODO #406 Need to fix
		LOGGER.warn("This log level is currently hardcoded. Need to fix it later.");
		return Level.INFO;
	}

	protected static void validateOptions(
			final JobContext context )
			throws IOException {
		if (getOperations(context) == null) {
			LOGGER.warn("Zookeeper connection for accumulo is null");
			throw new IOException(
					"Zookeeper connection for accumulo is null");
		}
	}

	protected static Integer getMinimumSplitCount(
			final JobContext context ) {
		return GeoWaveHBaseInputConfigurator.getMinimumSplitCount(
				CLASS,
				context);
	}

	protected static Integer getMaximumSplitCount(
			final JobContext context ) {
		return GeoWaveHBaseInputConfigurator.getMaximumSplitCount(
				CLASS,
				context);
	}

}
