/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase.mapreduce;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.datastore.hbase.mapreduce.input.GeoWaveHBaseInputFormat;
import mil.nga.giat.geowave.datastore.hbase.mapreduce.output.GeoWaveHBaseOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

/**
 * @author viggy Functionality similar to <code> GeoWaveJobRunner </code>
 */
public abstract class GeoWaveHBaseJobRunner extends
		Configured implements
		Tool
{
	protected static final Logger LOGGER = Logger.getLogger(GeoWaveHBaseJobRunner.class);
	protected String zookeeper;
	protected String namespace;
	protected List<DataAdapter<?>> adapters = new ArrayList<DataAdapter<?>>();
	protected List<Index> indices = new ArrayList<Index>();
	protected DistributableQuery query = null;
	protected Integer minInputSplits = null;
	protected Integer maxInputSplits = null;

	@Override
	public int run(
			final String[] args )
			throws Exception {
		zookeeper = args[0];
		namespace = args[1];
		return runJob();
	}

	@SuppressWarnings("deprecation")
	public int runJob()
			throws Exception {
		final Job job = new Job(
				super.getConf());
		// must use the assembled job configuration
		final Configuration conf = job.getConfiguration();

		GeoWaveHBaseInputFormat.setOperationsInfo(
				job,
				zookeeper,
				namespace);

		GeoWaveHBaseOutputFormat.setOperationsInfo(
				job,
				zookeeper,
				namespace);

		job.setJarByClass(this.getClass());

		configure(job);

		if ((adapters != null) && (adapters.size() > 0)) {
			for (final DataAdapter<?> adapter : adapters) {
				GeoWaveHBaseInputFormat.addDataAdapter(
						conf,
						adapter);
			}
		}
		if ((indices != null) && (indices.size() > 0)) {
			for (final Index index : indices) {
				GeoWaveHBaseInputFormat.addIndex(
						conf,
						index);
			}
		}
		if (query != null) {
			GeoWaveHBaseInputFormat.setQuery(
					conf,
					query);
		}
		if (minInputSplits != null) {
			GeoWaveHBaseInputFormat.setMinimumSplitCount(
					conf,
					minInputSplits);
		}
		if (maxInputSplits != null) {
			GeoWaveHBaseInputFormat.setMaximumSplitCount(
					conf,
					maxInputSplits);
		}

		final boolean jobSuccess = job.waitForCompletion(true);

		return (jobSuccess) ? 0 : 1;
	}

	public void setMinInputSplits(
			final int minInputSplits ) {
		this.minInputSplits = minInputSplits;
	}

	public void setMaxInputSplits(
			final int maxInputSplits ) {
		this.maxInputSplits = maxInputSplits;
	}

	public void setQuery(
			final DistributableQuery query ) {
		this.query = query;
	}

	public void addDataAdapter(
			final DataAdapter<?> adapter ) {
		adapters.add(adapter);
	}

	public void addIndex(
			final Index index ) {
		indices.add(index);
	}

	protected abstract void configure(
			Job job )
			throws Exception;
}
