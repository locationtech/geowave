package mil.nga.giat.geowave.ingest.hdfs.mapreduce;

import mil.nga.giat.geowave.accumulo.AccumuloAdapterStore;
import mil.nga.giat.geowave.accumulo.AccumuloIndexStore;
import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.ByteArrayUtils;
import mil.nga.giat.geowave.index.PersistenceUtils;
import mil.nga.giat.geowave.ingest.AccumuloCommandLineOptions;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexStore;

import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;

public class IngestWithMapperJobRunner extends
		Configured implements
		Tool
{
	private static String JOB_NAME = "%s Ingest from %s to namespace %s (map only)";
	private final AccumuloCommandLineOptions accumuloOptions;
	private final Path inputFile;
	private final String typeName;
	private final IngestFromHdfsPlugin plugin;
	private final IngestWithMapper mapperIngest;

	public IngestWithMapperJobRunner(
			final AccumuloCommandLineOptions accumuloOptions,
			final Path inputFile,
			final String typeName,
			final IngestFromHdfsPlugin plugin,
			final IngestWithMapper mapperIngest ) {
		this.accumuloOptions = accumuloOptions;
		this.inputFile = inputFile;
		this.typeName = typeName;
		this.plugin = plugin;
		this.mapperIngest = mapperIngest;
	}

	public String getJobName() {
		return String.format(
				JOB_NAME,
				typeName,
				inputFile.toString(),
				accumuloOptions.getNamespace());
	}

	@Override
	public int run(
			final String[] args )
			throws Exception {
		final Configuration conf = getConf();
		conf.set(
				IngestMapper.INGEST_WITH_MAPPER_KEY,
				ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(mapperIngest)));
		if (accumuloOptions.getVisibility() != null) {
			conf.set(
					IngestMapper.GLOBAL_VISIBILITY_KEY,
					accumuloOptions.getVisibility());
		}

		final Job job = new Job(
				conf,
				getJobName());

		job.setJarByClass(IngestWithMapperJobRunner.class);
		job.setMapperClass(IngestMapper.class);

		job.setInputFormatClass(AvroKeyInputFormat.class);
		AvroJob.setInputKeySchema(
				job,
				plugin.getAvroSchemaForHdfsType());
		FileInputFormat.setInputPaths(
				job,
				inputFile);

		// set mappper output info
		job.setMapOutputKeyClass(ByteArrayId.class);
		job.setMapOutputValueClass(Object.class);

		// set geowave output format
		job.setOutputFormatClass(GeoWaveOutputFormat.class);

		job.setNumReduceTasks(0);

		// set accumulo operations
		GeoWaveOutputFormat.setAccumuloOperationsInfo(
				job,
				accumuloOptions.getZookeepers(), // zookeepers
				accumuloOptions.getInstanceId(), // accumuloInstance
				accumuloOptions.getUser(), // accumuloUser
				accumuloOptions.getPassword(), // accumuloPass
				accumuloOptions.getNamespace()); // geowaveNamespace
		// TODO have the outputformat handle the initial creation of adapter and
		// index
		final AccumuloOperations operations = accumuloOptions.getAccumuloOperations();

		final AdapterStore adapterStore = new AccumuloAdapterStore(
				operations);
		final IndexStore indexStore = new AccumuloIndexStore(
				operations);

		final WritableDataAdapter<?>[] dataAdapters = mapperIngest.getDataAdapters(accumuloOptions.getVisibility());
		for (final WritableDataAdapter<?> dataAdapter : dataAdapters) {
			adapterStore.addAdapter(dataAdapter);
			GeoWaveOutputFormat.addDataAdapter(
					job,
					dataAdapter);
		}
		final Index index = accumuloOptions.getIndex();
		indexStore.addIndex(index);

		// set index
		GeoWaveOutputFormat.setIndex(
				job,
				index);

		return job.waitForCompletion(true) ? 0 : -1;
	}
}
