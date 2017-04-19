package mil.nga.giat.geowave.datastore.bigtable.operations;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

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

	@Override
	public ResultScanner getScannedResults(
			Scan scanner,
			String tableName,
			String... authorizations )
			throws IOException {

		if (tableExists(tableName)) {
			// TODO Cache locally b/c numerous checks can be expensive
			return super.getScannedResults(
					scanner,
					tableName,
					authorizations);
		}
		return new ResultScanner() {
			@Override
			public Iterator<Result> iterator() {
				return Collections.emptyIterator();
			}

			@Override
			public Result[] next(
					int nbRows )
					throws IOException {
				return null;
			}

			@Override
			public Result next()
					throws IOException {
				return null;
			}

			@Override
			public void close() {}
		};
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
