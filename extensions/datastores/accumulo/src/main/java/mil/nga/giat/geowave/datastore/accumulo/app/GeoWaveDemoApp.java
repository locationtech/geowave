package mil.nga.giat.geowave.datastore.accumulo.app;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
// @formatter:off
/*if_not[ACCUMULO_1.5.2]
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.monitor.Monitor;
end[ACCUMULO_1.5.2]*/
// @formatter:on
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.common.io.Files;

public class GeoWaveDemoApp
{

	public static void main(
			final String[] args )
			throws Exception {
		Logger.getRootLogger().setLevel(
				Level.WARN);

		final boolean interactive = (System.getProperty("interactive") != null) ? Boolean.parseBoolean(System.getProperty("interactive")) : true;

		final String password = (System.getProperty("password") != null) ? System.getProperty("password") : "password";

		final File tempDir = Files.createTempDir();
		// @formatter:off
		/*if_not[ACCUMULO_1.5.2]
		final String instanceName = (System.getProperty("instanceName") != null) ? System.getProperty("instanceName") : "geowave";
		final MiniAccumuloConfigImpl miniAccumuloConfig = new MiniAccumuloConfigImpl(
				tempDir,
				password).setNumTservers(
				2).setInstanceName(
				instanceName).setZooKeeperPort(
				2181);

		miniAccumuloConfig.setProperty(
				Property.MONITOR_PORT,
				"50095");

		final MiniAccumuloClusterImpl accumulo = new MiniAccumuloClusterImpl(
				miniAccumuloConfig);
  		else[ACCUMULO_1.5.2]*/
		final MiniAccumuloCluster accumulo = new MiniAccumuloCluster(
				new MiniAccumuloConfig(
						tempDir,
						password).setNumTservers(
						2));
		/*end[ACCUMULO_1.5.2]*/
		// @formatter:on
		accumulo.start();

		// @formatter:off
		/*if_not[ACCUMULO_1.5.2]
		accumulo.exec(Monitor.class);
		end[ACCUMULO_1.5.2]*/
		// @formatter:on

		System.out.println("starting up ...");
		Thread.sleep(3000);

		System.out.println("cluster running with instance name " + accumulo.getInstanceName() + " and zookeepers " + accumulo.getZooKeepers());

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
