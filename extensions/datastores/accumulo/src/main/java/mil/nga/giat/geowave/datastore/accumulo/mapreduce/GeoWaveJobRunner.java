package mil.nga.giat.geowave.datastore.accumulo.mapreduce;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.output.GeoWaveOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

/**
 * This class can run a basic job to query GeoWave. It manages Accumulo user,
 * Accumulo password, Accumulo instance name, zookeeper URLs, Accumulo
 * namespace, adapters, indices, query, min splits and max splits.
 */
public abstract class GeoWaveJobRunner extends
		Configured implements
		Tool
{

	protected static final Logger LOGGER = Logger.getLogger(GeoWaveJobRunner.class);

	protected String user;
	protected String password;
	protected String instance;
	protected String zookeeper;
	protected String namespace;
	protected List<DataAdapter<?>> adapters = new ArrayList<DataAdapter<?>>();
	protected List<Index> indices = new ArrayList<Index>();
	protected DistributableQuery query = null;
	protected Integer minInputSplits = null;
	protected Integer maxInputSplits = null;

	/**
	 * Main method to execute the MapReduce analytic.
	 */
	@SuppressWarnings("deprecation")
	public int runJob()
			throws Exception {
		final Job job = new Job(
				super.getConf());
		// must use the assembled job configuration
		final Configuration conf = job.getConfiguration();

		GeoWaveInputFormat.setAccumuloOperationsInfo(
				job,
				zookeeper,
				instance,
				user,
				password,
				namespace);

		GeoWaveOutputFormat.setAccumuloOperationsInfo(
				job,
				zookeeper,
				instance,
				user,
				password,
				namespace);

		job.setJarByClass(this.getClass());

		configure(job);

		if ((adapters != null) && (adapters.size() > 0)) {
			for (final DataAdapter<?> adapter : adapters) {
				GeoWaveInputFormat.addDataAdapter(
						conf,
						adapter);
			}
		}
		if ((indices != null) && (indices.size() > 0)) {
			for (final Index index : indices) {
				GeoWaveInputFormat.addIndex(
						conf,
						index);
			}
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

	public void addDataAdapter(
			final DataAdapter<?> adapter ) {
		adapters.add(adapter);
	}

	public void addIndex(
			final Index index ) {
		indices.add(index);
	}

	public void setQuery(
			final DistributableQuery query ) {
		this.query = query;
	}

	@Override
	public int run(
			final String[] args )
			throws Exception {
		zookeeper = args[0];
		instance = args[1];
		user = args[2];
		password = args[3];
		namespace = args[4];
		return runJob();
	}
}
