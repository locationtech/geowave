package mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.hbase;

import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.ingest.DataAdapterProvider;
import mil.nga.giat.geowave.core.ingest.IngestCommandLineOptions;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.hbase.HBaseCommandLineOptions;
import mil.nga.giat.geowave.datastore.hbase.mapreduce.output.GeoWaveHBaseOutputFormat;

import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;

/**
 * @author viggy Functionality similar to <code> AbstractMapReduceIngest </code>
 */
abstract public class AbstractMapReduceHBaseIngest<T extends Persistable & DataAdapterProvider> extends
		Configured implements
		Tool
{
	public static final String INGEST_PLUGIN_KEY = "INGEST_PLUGIN";
	public static final String GLOBAL_VISIBILITY_KEY = "GLOBAL_VISIBILITY";
	public static final String PRIMARY_INDEX_ID_KEY = "PRIMARY_INDEX_ID";
	private static String JOB_NAME = "%s ingest from %s to namespace %s (%s)";
	protected final HBaseCommandLineOptions hbaseOptions;
	protected final IngestCommandLineOptions ingestOptions;
	protected final Path inputFile;
	protected final String typeName;
	protected final IngestFromHdfsPlugin parentPlugin;
	protected final T ingestPlugin;

	public AbstractMapReduceHBaseIngest(
			final HBaseCommandLineOptions accumuloOptions,
			final IngestCommandLineOptions ingestOptions,
			final Path inputFile,
			final String typeName,
			final IngestFromHdfsPlugin parentPlugin,
			final T ingestPlugin ) {
		this.hbaseOptions = accumuloOptions;
		this.ingestOptions = ingestOptions;
		this.inputFile = inputFile;
		this.typeName = typeName;
		this.parentPlugin = parentPlugin;
		this.ingestPlugin = ingestPlugin;
	}

	public String getJobName() {
		return String.format(
				JOB_NAME,
				typeName,
				inputFile.toString(),
				hbaseOptions.getNamespace(),
				getIngestDescription());
	}

	abstract protected String getIngestDescription();

	@Override
	public int run(
			final String[] args )
			throws Exception {
		final Configuration conf = getConf();
		conf.set(
				INGEST_PLUGIN_KEY,
				ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(ingestPlugin)));
		if (ingestOptions.getVisibility() != null) {
			conf.set(
					GLOBAL_VISIBILITY_KEY,
					ingestOptions.getVisibility());
		}
		final Index primaryIndex = ingestOptions.getIndex(parentPlugin.getSupportedIndices());
		if (primaryIndex != null) {
			conf.set(
					PRIMARY_INDEX_ID_KEY,
					StringUtils.stringFromBinary(primaryIndex.getId().getBytes()));
		}
		final Job job = new Job(
				conf,
				getJobName());

		job.setJarByClass(AbstractMapReduceHBaseIngest.class);

		job.setInputFormatClass(AvroKeyInputFormat.class);
		AvroJob.setInputKeySchema(
				job,
				parentPlugin.getAvroSchema());
		FileInputFormat.setInputPaths(
				job,
				inputFile);

		setupMapper(job);
		setupReducer(job);
		// set geowave output format
		job.setOutputFormatClass(GeoWaveHBaseOutputFormat.class);

		GeoWaveHBaseOutputFormat.setOperationsInfo(
				job,
				hbaseOptions.getZookeepers(), // zookeepers
				hbaseOptions.getNamespace()); // geowaveNamespace

		final WritableDataAdapter<?>[] dataAdapters = ingestPlugin.getDataAdapters(ingestOptions.getVisibility());
		for (final WritableDataAdapter<?> dataAdapter : dataAdapters) {
			GeoWaveHBaseOutputFormat.addDataAdapter(
					job.getConfiguration(),
					dataAdapter);
		}

		job.setSpeculativeExecution(false);

		// add primary index
		GeoWaveHBaseOutputFormat.addIndex(
				job.getConfiguration(),
				primaryIndex);

		// add required indices
		final Index[] requiredIndices = parentPlugin.getRequiredIndices();
		if (requiredIndices != null) {
			for (final Index requiredIndex : requiredIndices) {
				GeoWaveHBaseOutputFormat.addIndex(
						job.getConfiguration(),
						requiredIndex);
			}
		}
		return job.waitForCompletion(true) ? 0 : -1;
	}

	abstract protected void setupMapper(
			Job job );

	abstract protected void setupReducer(
			Job job );
}
