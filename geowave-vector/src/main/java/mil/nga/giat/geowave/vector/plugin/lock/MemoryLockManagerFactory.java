package mil.nga.giat.geowave.vector.plugin.lock;

import mil.nga.giat.geowave.vector.plugin.GeoWavePluginConfig;

public class MemoryLockManagerFactory implements
		LockingManagementFactory
{

	@Override
	public LockingManagement createLockingManager(
			GeoWavePluginConfig plugginData ) {
		return new MemoryLockManager(
				plugginData);
	}

	@Override
	public String toString() {
		return "memory";
	}

}
