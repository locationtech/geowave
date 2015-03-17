package mil.nga.giat.geowave.raster.resize;

import mil.nga.giat.geowave.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.accumulo.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.accumulo.mapreduce.JobContextAdapterStore;
import mil.nga.giat.geowave.accumulo.mapreduce.JobContextIndexStore;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.accumulo.mapreduce.output.GeoWaveOutputFormat;
import mil.nga.giat.geowave.accumulo.mapreduce.output.GeoWaveOutputKey;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.raster.adapter.merge.nodata.NoDataMergeStrategy;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.DataStore;
import mil.nga.giat.geowave.store.IndexWriter;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.index.Index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.opengis.coverage.grid.GridCoverage;

public class RasterTileResizeJobRunner extends
		Configured implements
		Tool
{
	public static final String NEW_ADAPTER_ID_KEY = "NEW_ADAPTER_ID";
	public static final String OLD_ADAPTER_ID_KEY = "OLD_ADAPTER_ID";

	protected String user;
	protected String password;
	protected String instance;
	protected String zookeeper;

	protected String oldNamespace;
	protected String oldCoverageName;
	protected String newNamespace;
	protected String newCoverageName;

	protected int minSplits;
	protected int maxSplits;
	protected int newTileSize;

	protected String hdfsHostPort;
	protected String jobTrackerOrResourceManHostPort;
	protected String indexId;

	public RasterTileResizeJobRunner() {

	}

	/**
	 * Main method to execute the MapReduce analytic.
	 */
	public int runJob()
			throws Exception {
		final Configuration conf = super.getConf();
		GeoWaveConfiguratorBase.setRemoteInvocationParams(
				hdfsHostPort,
				jobTrackerOrResourceManHostPort,
				conf);
		conf.set(
				OLD_ADAPTER_ID_KEY,
				oldCoverageName);
		conf.set(
				NEW_ADAPTER_ID_KEY,
				newCoverageName);
		final Job job = new Job(
				conf);

		job.setJarByClass(this.getClass());

		job.setJobName("Converting " + oldCoverageName + " to tile size=" + newTileSize);

		job.setMapperClass(RasterTileResizeMapper.class);
		job.setCombinerClass(RasterTileResizeCombiner.class);
		job.setReducerClass(RasterTileResizeReducer.class);
		job.setInputFormatClass(GeoWaveInputFormat.class);
		job.setOutputFormatClass(GeoWaveOutputFormat.class);
		job.setMapOutputKeyClass(GeoWaveInputKey.class);
		job.setMapOutputValueClass(ObjectWritable.class);
		job.setOutputKeyClass(GeoWaveOutputKey.class);
		job.setOutputValueClass(GridCoverage.class);
		job.setNumReduceTasks(8);

		GeoWaveInputFormat.setMinimumSplitCount(
				job.getConfiguration(),
				minSplits);
		GeoWaveInputFormat.setMaximumSplitCount(
				job.getConfiguration(),
				maxSplits);
		GeoWaveInputFormat.setAccumuloOperationsInfo(
				job,
				zookeeper,
				instance,
				user,
				password,
				oldNamespace);

		GeoWaveOutputFormat.setAccumuloOperationsInfo(
				job,
				zookeeper,
				instance,
				user,
				password,
				newNamespace);
		final AccumuloOperations oldNamespaceOperations = new BasicAccumuloOperations(
				zookeeper,
				instance,
				user,
				password,
				oldNamespace);
		final DataAdapter adapter = new AccumuloAdapterStore(
				oldNamespaceOperations).getAdapter(new ByteArrayId(
				oldCoverageName));
		if (adapter == null) {
			throw new IllegalArgumentException(
					"Adapter for coverage '" + oldCoverageName + "' does not exist in namespace '" + oldNamespace + "'");
		}

		final RasterDataAdapter newAdapter = new RasterDataAdapter(
				(RasterDataAdapter) adapter,
				newCoverageName,
				newTileSize,
				new NoDataMergeStrategy());
		JobContextAdapterStore.addDataAdapter(
				job.getConfiguration(),
				adapter);
		JobContextAdapterStore.addDataAdapter(
				job.getConfiguration(),
				newAdapter);
		Index index = null;
		if (indexId != null) {
			index = new AccumuloIndexStore(
					oldNamespaceOperations).getIndex(new ByteArrayId(
					indexId));
		}
		if (index == null) {
			try (CloseableIterator<Index> indices = new AccumuloIndexStore(
					oldNamespaceOperations).getIndices()) {
				index = indices.next();
			}
			if (index == null) {
				throw new IllegalArgumentException(
						"Index does not exist in namespace '" + oldNamespaceOperations + "'");
			}
		}
		JobContextIndexStore.addIndex(
				job.getConfiguration(),
				index);
		final AccumuloOperations ops = new BasicAccumuloOperations(
				zookeeper,
				instance,
				user,
				password,
				newNamespace);
		final DataStore store = new AccumuloDataStore(
				ops);
		final IndexWriter writer = store.createIndexWriter(index);
		writer.setupAdapter(newAdapter);
		Boolean retVal = job.waitForCompletion(true);
		writer.close();
		return retVal ? 0 : 1;
	}

	public static void main(
			final String[] args )
			throws Exception {
		final int res = ToolRunner.run(
				new Configuration(),
				new RasterTileResizeJobRunner(),
				args);
		System.exit(res);
	}

	@Override
	public int run(
			final String[] args )
			throws Exception {
		if (args.length > 0) {
			zookeeper = args[0];
			instance = args[1];
			user = args[2];
			password = args[3];
			oldNamespace = args[4];
			oldCoverageName = args[5];
			minSplits = Integer.parseInt(args[6]);
			maxSplits = Integer.parseInt(args[7]);
			hdfsHostPort = args[8];
			if (!hdfsHostPort.contains("://")) {
				hdfsHostPort = "hdfs://" + hdfsHostPort;
			}
			jobTrackerOrResourceManHostPort = args[9];
			newCoverageName = args[10];
			newNamespace = args[11];
			newTileSize = Integer.parseInt(args[12]);
			if (args.length > 13) {
				indexId = args[13];
			}
		}
		return runJob();
	}

}