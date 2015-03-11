package mil.nga.giat.geowave.benchmark.utils;

import mil.nga.giat.geowave.accumulo.util.ConnectorPool;

import org.apache.accumulo.core.client.Connector;

public class CachingSetup
{
	private final String zookeeperUrl;
	private final String instancename;
	private final String username;
	private final String password;

	final private int[] pointsPerTile = new int[] {
		0,
		100,
		500,
		1000,
		5000,
		10000,
		50000
	};

	final private String[] indexTypes = {
		"vector",
		"raster"
	};

	public CachingSetup() {
		zookeeperUrl = System.getProperty("zookeeperUrl");
		instancename = System.getProperty("instance");
		username = System.getProperty("username");
		password = System.getProperty("password");
	}

	public void setupCaching(
			final boolean enable )
			throws Exception {

		final Connector connector = ConnectorPool.getInstance().getConnector(
				zookeeperUrl,
				instancename,
				username,
				password);

		for (final String indexType : indexTypes) {
			for (final int tileSize : pointsPerTile) {
				final String[] namespaces;
				if (tileSize == 0) {
					namespaces = new String[] {
						"featureTest_" + indexType + "_SPATIAL_" + indexType.toUpperCase() + "_INDEX",
						"featureTest_" + indexType + "_GEOWAVE_METADATA",
						"featureTest_" + indexType + "_SPATIAL_" + indexType.toUpperCase() + "_INDEX_GEOWAVE_ALT_INDEX"
					};
				}
				else {
					namespaces = new String[] {
						"featureCollectionTest_" + indexType + "_" + tileSize + "_SPATIAL_" + indexType.toUpperCase() + "_INDEX",
						"featureCollectionTest_" + indexType + "_" + tileSize + "_GEOWAVE_METADATA",
						"featureCollectionTest_" + indexType + "_" + tileSize + "_SPATIAL_" + indexType.toUpperCase() + "_INDEX_GEOWAVE_ALT_INDEX"
					};
				}

				for (final String namespace : namespaces) {
					System.out.println("Configuring: [" + namespace + "]");

					// compact the table
					connector.tableOperations().compact(
							namespace,
							null,
							null,
							true,
							false);

					// disable block caching
					connector.tableOperations().setProperty(
							namespace,
							"table.cache.block.enable",
							Boolean.toString(enable));

					// disable index caching
					connector.tableOperations().setProperty(
							namespace,
							"table.cache.index.enable",
							Boolean.toString(enable));
				}
			}
		}

	}

	public static void main(
			final String[] args )
			throws Exception {
		final CachingSetup t = new CachingSetup();
		t.setupCaching(false);
	}

}
