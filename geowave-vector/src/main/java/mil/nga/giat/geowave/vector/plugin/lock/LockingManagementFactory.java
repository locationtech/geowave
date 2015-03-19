package mil.nga.giat.geowave.vector.plugin.lock;

import mil.nga.giat.geowave.vector.plugin.GeoWavePluginConfig;

/**
 * Factories are used with the {@link java.util.ServiceLoader) approach to
 * discover locking management strategies. * *
 */
public interface LockingManagementFactory
{

	public LockingManagement createLockingManager(
			GeoWavePluginConfig plugginData );
}
