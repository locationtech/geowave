package mil.nga.giat.geowave.datastore.bigtable.operations;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;

import mil.nga.giat.geowave.datastore.bigtable.operations.config.BigTableOptions;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;

public class BigTableOperations extends
		BasicHBaseOperations
{

	public BigTableOperations()
			throws IOException {
		this(
				DEFAULT_TABLE_NAMESPACE);
	}

	public BigTableOperations(
			final String geowaveNamespace )
			throws IOException {
		this(
				BigTableOptions.DEFAULT_PROJECT_ID,
				BigTableOptions.DEFAULT_INSTANCE_ID,
				geowaveNamespace);
	}

	public BigTableOperations(
			final String projectId,
			final String instanceId,
			final String geowaveNamespace )
			throws IOException {
		super(
				getConnection(
						projectId,
						instanceId),
				geowaveNamespace);
	}

	private static Connection getConnection(
			final String projectId,
			final String instanceId ) {

		final Configuration config = BigtableConfiguration.configure(
				projectId,
				instanceId);

		// TODO: Bigtable configgy things? What about connection pooling?
		config.setBoolean(
				"hbase.online.schema.update.enable",
				true);

		return BigtableConfiguration.connect(config);
	}

	public static BigTableOperations createOperations(
			final BigTableOptions options )
			throws IOException {
		return new BigTableOperations(
				options.getProjectId(),
				options.getInstanceId(),
				options.getGeowaveNamespace());
	}

}
