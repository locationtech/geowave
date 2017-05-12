package mil.nga.giat.geowave.mapreduce;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputFormat;

/**
 * This class can run a basic job to query GeoWave. It manages datastore
 * connection params, adapters, indices, query, min splits and max splits.
 */
public abstract class AbstractGeoWaveJobRunner extends
		Configured implements
		Tool
{

	protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractGeoWaveJobRunner.class);

	protected DataStorePluginOptions dataStoreOptions;
	protected DistributableQuery query = null;
	protected QueryOptions queryOptions;
	protected Integer minInputSplits = null;
	protected Integer maxInputSplits = null;

	public AbstractGeoWaveJobRunner(
			final DataStorePluginOptions dataStoreOptions ) {
		this.dataStoreOptions = dataStoreOptions;
	}

	/**
	 * Main method to execute the MapReduce analytic.
	 */
	public int runJob()
			throws Exception {
		final Job job = Job.getInstance(super.getConf());
		// must use the assembled job configuration
		final Configuration conf = job.getConfiguration();

		GeoWaveInputFormat.setStoreOptions(
				conf,
				dataStoreOptions);

		GeoWaveOutputFormat.setStoreOptions(
				conf,
				dataStoreOptions);

		job.setJarByClass(this.getClass());

		configure(job);

		if (queryOptions != null) {
			GeoWaveInputFormat.setQueryOptions(
					conf,
					queryOptions);

		}
		if (query != null) {
			GeoWaveInputFormat.setQuery(
					conf,
					query);
		}
		if (minInputSplits != null) {
			GeoWaveInputFormat.setMinimumSplitCount(
					conf,
					minInputSplits);
		}
		if (maxInputSplits != null) {
			GeoWaveInputFormat.setMaximumSplitCount(
					conf,
					maxInputSplits);
		}

		final boolean jobSuccess = job.waitForCompletion(true);

		return (jobSuccess) ? 0 : 1;
	}

	protected abstract void configure(
			Job job )
			throws Exception;

	public void setMaxInputSplits(
			final int maxInputSplits ) {
		this.maxInputSplits = maxInputSplits;
	}

	public void setMinInputSplits(
			final int minInputSplits ) {
		this.minInputSplits = minInputSplits;
	}

	public void setQueryOptions(
			final QueryOptions options ) {
		queryOptions = options;
	}

	public void setQuery(
			final DistributableQuery query ) {
		this.query = query;
	}

	@Override
	public int run(
			final String[] args )
			throws Exception {
		return runOperation(args) ? 0 : -1;
	}

	public boolean runOperation(
			final String[] args )
			throws ParseException {
		try {
			return runJob() == 0 ? true : false;
		}
		catch (final Exception e) {
			LOGGER.error(
					"Unable to run job",
					e);
			throw new ParseException(
					e.getMessage());
		}
	}

}
