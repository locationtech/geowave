package mil.nga.giat.geowave.cli.osm.mapreduce.Ingest;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.cli.osm.accumulo.osmschema.ColumnFamily;
import mil.nga.giat.geowave.cli.osm.operations.options.OSMIngestCommandArgs;
import mil.nga.giat.geowave.cli.osm.types.generated.Node;
import mil.nga.giat.geowave.cli.osm.types.generated.Relation;
import mil.nga.giat.geowave.cli.osm.types.generated.Way;
import mil.nga.giat.geowave.core.cli.parser.CommandLineOperationParams;
import mil.nga.giat.geowave.core.cli.parser.OperationParser;
import mil.nga.giat.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloStoreFactoryFamily;
import mil.nga.giat.geowave.datastore.accumulo.cli.config.AccumuloOptions;
import mil.nga.giat.geowave.datastore.accumulo.cli.config.AccumuloRequiredOptions;
import mil.nga.giat.geowave.datastore.accumulo.operations.AccumuloOperations;

public class OSMRunner extends
		Configured implements
		Tool
{
	private static final Logger log = LoggerFactory.getLogger(OSMRunner.class);
	private org.apache.avro.Schema avroSchema = null;
	private String inputAvroFile = null;

	private final OSMIngestCommandArgs ingestOptions;
	private final AccumuloRequiredOptions accumuloOptions;

	public static void main(
			final String[] args )
			throws Exception {
		final OSMIngestCommandArgs argv = new OSMIngestCommandArgs();
		final DataStorePluginOptions opts = new DataStorePluginOptions();
		opts.selectPlugin(new AccumuloStoreFactoryFamily().getType());

		final OperationParser parser = new OperationParser();
		parser.addAdditionalObject(argv);
		parser.addAdditionalObject(opts);

		final CommandLineOperationParams params = parser.parse(args);
		if (params.getSuccessCode() == 0) {
			final OSMRunner runner = new OSMRunner(
					argv,
					opts);
			final int res = ToolRunner.run(
					new Configuration(),
					runner,
					args);
			System.exit(res);
		}

		System.out.println(params.getSuccessMessage());
		System.exit(params.getSuccessCode());
	}

	public OSMRunner(
			final OSMIngestCommandArgs ingestOptions,
			final DataStorePluginOptions inputStoreOptions ) {
		this.ingestOptions = ingestOptions;
		if (!inputStoreOptions.getType().equals(
				new AccumuloStoreFactoryFamily().getType())) {
			throw new RuntimeException(
					"Expected accumulo data store");
		}
		accumuloOptions = (AccumuloRequiredOptions) inputStoreOptions.getFactoryOptions();

	}

	public void configureSchema(
			final org.apache.avro.Schema avroSchema ) {
		this.avroSchema = avroSchema;
	}

	private void enableLocalityGroups(
			final OSMIngestCommandArgs argv )
			throws AccumuloSecurityException,
			AccumuloException,
			TableNotFoundException {
		final AccumuloOperations bao = new AccumuloOperations(
				accumuloOptions.getZookeeper(),
				accumuloOptions.getInstance(),
				accumuloOptions.getUser(),
				accumuloOptions.getPassword(),
				accumuloOptions.getGeowaveNamespace(),
				new AccumuloOptions());
		bao.createTable(
				argv.getOsmTableName(),
				true,
				true);

		bao.addLocalityGroup(
				argv.getOsmTableName(),
				ColumnFamily.NODE);
		bao.addLocalityGroup(
				argv.getOsmTableName(),
				ColumnFamily.WAY);
		bao.addLocalityGroup(
				argv.getOsmTableName(),
				ColumnFamily.RELATION);
	}

	@Override
	public int run(
			final String[] args )
			throws Exception {

		final Configuration conf = getConf();
		conf.set(
				"tableName",
				ingestOptions.getQualifiedTableName());
		conf.set(
				"osmVisibility",
				ingestOptions.getVisibilityOptions().getVisibility());

		// job settings
		final Job job = Job.getInstance(
				conf,
				ingestOptions.getJobName());
		job.setJarByClass(OSMRunner.class);

		switch (ingestOptions.getMapperType()) {
			case "NODE": {
				configureSchema(Node.getClassSchema());
				inputAvroFile = ingestOptions.getNameNode() + "/" + ingestOptions.getNodesBasePath();
				job.setMapperClass(OSMNodeMapper.class);
				break;
			}
			case "WAY": {
				configureSchema(Way.getClassSchema());
				inputAvroFile = ingestOptions.getNameNode() + "/" + ingestOptions.getWaysBasePath();
				job.setMapperClass(OSMWayMapper.class);
				break;
			}
			case "RELATION": {
				configureSchema(Relation.getClassSchema());
				inputAvroFile = ingestOptions.getNameNode() + "/" + ingestOptions.getRelationsBasePath();
				job.setMapperClass(OSMRelationMapper.class);
				break;
			}
			default:
				break;
		}
		if ((avroSchema == null) || (inputAvroFile == null)) {
			throw new MissingArgumentException(
					"argument for mapper type must be one of: NODE, WAY, or RELATION");
		}

		enableLocalityGroups(ingestOptions);

		// input format
		job.setInputFormatClass(AvroKeyInputFormat.class);
		FileInputFormat.setInputPaths(
				job,
				inputAvroFile);
		AvroJob.setInputKeySchema(
				job,
				avroSchema);

		// mappper

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Mutation.class);
		job.setOutputFormatClass(AccumuloOutputFormat.class);
		AccumuloOutputFormat.setConnectorInfo(
				job,
				accumuloOptions.getUser(),
				new PasswordToken(
						accumuloOptions.getPassword()));
		AccumuloOutputFormat.setCreateTables(
				job,
				true);
		AccumuloOutputFormat.setDefaultTableName(
				job,
				ingestOptions.getQualifiedTableName());
		AccumuloOutputFormat.setZooKeeperInstance(
				job,
				new ClientConfiguration().withInstance(
						accumuloOptions.getInstance()).withZkHosts(
						accumuloOptions.getZookeeper()));

		// reducer
		job.setNumReduceTasks(0);

		return job.waitForCompletion(true) ? 0 : -1;
	}

}
