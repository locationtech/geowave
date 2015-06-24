package mil.nga.giat.geowave.core.ingest;

import mil.nga.giat.geowave.core.cli.CLIOperation;
import mil.nga.giat.geowave.core.cli.CLIOperationCategory;
import mil.nga.giat.geowave.core.cli.CLIOperationProviderSpi;
import mil.nga.giat.geowave.core.ingest.hdfs.StageToHdfsDriver;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsDriver;
import mil.nga.giat.geowave.core.ingest.kafka.IngestFromKafkaDriver;
import mil.nga.giat.geowave.core.ingest.kafka.StageToKafkaDriver;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestDriver;

public class IngestCLIOperationProvider implements
		CLIOperationProviderSpi
{

	/**
	 * This identifies the set of operations supported and which driver to
	 * execute based on the operation selected.
	 */
	private static final CLIOperation[] INGEST_OPERATIONS = new CLIOperation[] {
		new CLIOperation(
				"clear",
				"clear ALL data from a GeoWave namespace, this actually deletes Accumulo tables prefixed by the given namespace",
				new ClearNamespaceDriver(
						"clear")),
		new CLIOperation(
				"localingest",
				"ingest supported files in local file system directly, without using HDFS",
				new LocalFileIngestDriver(
						"localingest")),
		new CLIOperation(
				"hdfsstage",
				"stage supported files in local file system to HDFS",
				new StageToHdfsDriver(
						"hdfsstage")),
		new CLIOperation(
				"poststage",
				"ingest supported files that already exist in HDFS",
				new IngestFromHdfsDriver(
						"poststage")),
		new CLIOperation(
				"hdfsingest",
				"copy supported files from local file system to HDFS and ingest from HDFS",
				new MultiStageCommandLineDriver(
						"hdfsingest",
						new AbstractIngestCommandLineDriver[] {
							new StageToHdfsDriver(
									"hdfsingest"),
							new IngestFromHdfsDriver(
									"hdfsingest")
						})),
		new CLIOperation(
				"kafkastage",
				"stage supported files in local file system to a Kafka topic",
				new StageToKafkaDriver(
						"kafkastage")),
		new CLIOperation(
				"kafkaingest",
				"subscribe to a Kafka topic and ingest into GeoWave",
				new IngestFromKafkaDriver(
						"kafkaingest")),
	};

	private static final CLIOperationCategory CATEGORY = new IngestOperationCategory();

	public IngestCLIOperationProvider() {}

	@Override
	public CLIOperation[] getOperations() {
		return INGEST_OPERATIONS;
	}

	@Override
	public CLIOperationCategory getCategory() {
		return CATEGORY;
	}
}
