package mil.nga.giat.geowave.vector.plugin.lock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.vector.plugin.GeoWavePluginConfig;


/**
 * Single GeoServer lock support. In a clustered model, do not use.
 * 
 *
 * 
 */
public class MemoryLockManager extends
		AbstractLockingManagement
{

	private static final Map<String, Map<String, AuthorizedLock>> LOCKS = new HashMap<String, Map<String, AuthorizedLock>>();
	private final Map<String, AuthorizedLock> locks;

	public MemoryLockManager(
			String instanceName ) {
		Map<String, AuthorizedLock> lockSet;
		synchronized (LOCKS) {
			lockSet = LOCKS.get(instanceName);
			if (lockSet == null) {
				lockSet = new HashMap<String, AuthorizedLock>();
				LOCKS.put(
						instanceName,
						lockSet);
			}
		}
		locks = lockSet;
	}

	public MemoryLockManager(
			GeoWavePluginConfig pluginConfig ) {
		this(
				pluginConfig.getInstanceName());
	}

	@Override
	public void releaseAll(
			AuthorizedLock lock ) {
		ArrayList<AuthorizedLock> toRelease = new ArrayList<AuthorizedLock>();
		synchronized (locks) {
			Iterator<Entry<String, AuthorizedLock>> it = locks.entrySet().iterator();
			while (it.hasNext()) {
				Entry<String, AuthorizedLock> entry = it.next();
				if (entry.getValue().equals(
						lock) || entry.getValue().isAuthorized(
						lock)) {
					toRelease.add(entry.getValue());
					it.remove();
				}
			}
		}
		for (AuthorizedLock lockToRelease : toRelease)
			lockToRelease.invalidate();
	}

	/**
	 * Release all locks associated with a transaction. Occurs on commit and
	 * rollback
	 * 
	 * @param lock
	 */
	@Override
	public void resetAll(
			AuthorizedLock lock ) {
		ArrayList<AuthorizedLock> toRelease = new ArrayList<AuthorizedLock>();
		synchronized (locks) {
			Iterator<Entry<String, AuthorizedLock>> it = locks.entrySet().iterator();
			while (it.hasNext()) {
				Entry<String, AuthorizedLock> entry = it.next();
				if (entry.getValue().equals(
						lock) || entry.getValue().isAuthorized(
						lock)) {
					toRelease.add(entry.getValue());
				}
			}
		}
		for (AuthorizedLock lockToRelease : toRelease)
			lockToRelease.resetExpireTime();
	}

	@Override
	public void lock(
			AuthorizedLock lock,
			String featureID ) {
		AuthorizedLock featureLock = null;

		synchronized (locks) {
			featureLock = locks.get(featureID);
			if (featureLock == null || featureLock.isStale()) {
				featureLock = lock;
				locks.put(
						featureID,
						lock);
				return;
			}
			else if (featureLock.isAuthorized(lock)) {
				return;
			}
		}
		while (featureLock != lock) {
			// at this point, some other transaction may have the lock
			synchronized (featureLock) {
				// check if stale, which occurs when the transaction is
				// completed.
				while (!featureLock.isStale())
					try {
						// only wait a little, because the feature could be
						// stale
						featureLock.wait(Math.min(
								5000,
								featureLock.getExpireTime() - System.currentTimeMillis()));
					}
					catch (InterruptedException ex) {}
					catch (Exception e) {
						e.printStackTrace();
					}
			}
			synchronized (locks) {
				featureLock = locks.get(featureID);
				if (featureLock == null || featureLock.isStale()) {
					locks.put(
							featureID,
							lock);
					featureLock = lock;
				}
			}
		}
	}

	@Override
	public boolean exists(
			String authID ) {
		synchronized (locks) {
			Iterator<Entry<String, AuthorizedLock>> it = locks.entrySet().iterator();
			while (it.hasNext()) {
				Entry<String, AuthorizedLock> entry = it.next();
				if (entry.getValue().isAuthorized(
						authID) || !entry.getValue().isStale()) return true;
			}
		}
		return false;
	}

	@Override
	public void unlock(
			AuthorizedLock lock,
			String featureID ) {
		AuthorizedLock featureLock = null;
		boolean notify = false;
		synchronized (locks) {
			featureLock = locks.get(featureID);
			if (featureLock != null && featureLock.isAuthorized(lock)) {
				locks.remove(featureID);
				notify = true;
			}
		}
		if (notify) {
			featureLock.invalidate();
		}
	}

}
