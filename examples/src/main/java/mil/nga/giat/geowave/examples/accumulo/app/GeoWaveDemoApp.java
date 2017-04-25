package mil.nga.giat.geowave.examples.accumulo.app;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.monitor.Monitor;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.VersionUtil;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.common.io.Files;

import mil.nga.giat.geowave.datastore.accumulo.minicluster.MiniAccumuloClusterFactory;
import mil.nga.giat.geowave.core.cli.operations.config.security.utils.SecurityUtils;

public class GeoWaveDemoApp
{
	private static final Logger LOGGER = Logger.getLogger(GeoWaveDemoApp.class);
	protected static final String HADOOP_WINDOWS_UTIL = "winutils.exe";

	protected static boolean isYarn() {
		return VersionUtil.compareVersions(
				VersionInfo.getVersion(),
				"2.2.0") >= 0;
	}

	public static void main(
			final String[] args )
			throws Exception {
		Logger.getRootLogger().setLevel(
				Level.WARN);

		final boolean interactive = (System.getProperty("interactive") != null) ? Boolean.parseBoolean(System
				.getProperty("interactive")) : true;

		// GeoWave:811 - providing ability to support encrypted passwords
		final String password = new SecurityUtils()
				.decryptHexEncodedValue(((System.getProperty("password") != null) ? System.getProperty("password")
						: "password"));

		final File tempDir = Files.createTempDir();
		final String instanceName = (System.getProperty("instanceName") != null) ? System.getProperty("instanceName")
				: "geowave";
		final MiniAccumuloConfigImpl miniAccumuloConfig = new MiniAccumuloConfigImpl(
				tempDir,
				password).setNumTservers(
				2).setInstanceName(
				instanceName).setZooKeeperPort(
				2181);

		miniAccumuloConfig.setProperty(
				Property.MONITOR_PORT,
				"50095");

		final MiniAccumuloClusterImpl accumulo = MiniAccumuloClusterFactory.newAccumuloCluster(
				miniAccumuloConfig,
				GeoWaveDemoApp.class);
		accumulo.start();

		accumulo.exec(Monitor.class);

		System.out.println("starting up ...");
		Thread.sleep(3000);

		System.out.println("cluster running with instance name " + accumulo.getInstanceName() + " and zookeepers "
				+ accumulo.getZooKeepers());

		if (interactive) {
			System.out.println("hit Enter to shutdown ..");
			System.in.read();
			System.out.println("Shutting down!");
			accumulo.stop();
		}
		else {
			Runtime.getRuntime().addShutdownHook(
					new Thread() {
						@Override
						public void run() {
							try {
								accumulo.stop();
							}
							catch (final Exception e) {
								LOGGER.warn(
										"Unable to shutdown accumulo",
										e);
								System.out.println("Error shutting down accumulo.");
							}
							System.out.println("Shutting down!");
						}
					});

			while (true) {
				Thread.sleep(TimeUnit.MILLISECONDS.convert(
						Long.MAX_VALUE,
						TimeUnit.DAYS));
			}
		}
	}
}
