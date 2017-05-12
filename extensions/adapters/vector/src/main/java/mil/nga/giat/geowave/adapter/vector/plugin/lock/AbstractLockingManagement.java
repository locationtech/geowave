package mil.nga.giat.geowave.adapter.vector.plugin.lock;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import mil.nga.giat.geowave.adapter.vector.plugin.GeoWavePluginConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.geotools.data.FeatureLock;
import org.geotools.data.Transaction;

/**
 * Simplifies Lock management from the more complex Geotools approach which is
 * used in several different scenarios (e.g. directory management, wfs-t, etc.)
 * 
 * Implementers implement three abstract methods. The Geotools still helps with
 * management, providing a locking source.
 * 
 * 
 */
public abstract class AbstractLockingManagement implements
		LockingManagement
{

	private final static Logger LOGGER = LoggerFactory.getLogger(AbstractLockingManagement.class);

	public static final String LOCKING_MANAGEMENT_CLASS = "GEOWAVE_LM";
	public static final Object LOCKING_MANAGEMENT_CLASS_LCK = new Object();

	public static AbstractLockingManagement getLockingManagement(
			GeoWavePluginConfig pluginConfig ) {
		synchronized (LOCKING_MANAGEMENT_CLASS_LCK) {
			String val = System.getenv(LOCKING_MANAGEMENT_CLASS);

			if (val == null) {
				return new MemoryLockManager(
						pluginConfig);
			}
			else {
				try {
					Class<? extends AbstractLockingManagement> lockManagerClass = (Class<? extends AbstractLockingManagement>) Class
							.forName(val);
					if (!AbstractLockingManagement.class.isAssignableFrom(lockManagerClass)) {
						throw new IllegalArgumentException(
								"Invalid LockManagement class " + val);
					}
					else {
						Constructor cons = lockManagerClass.getConstructor(GeoWavePluginConfig.class);
						return (AbstractLockingManagement) cons.newInstance(pluginConfig);
					}
				}
				catch (Exception ex) {
					// HP Fortify "Log Forging" false positive
					// What Fortify considers "user input" comes only
					// from users with OS-level access anyway
					LOGGER.error(
							"Cannot instantiate lock management class " + val,
							ex);
					return new MemoryLockManager(
							pluginConfig);
				}
			}
		}
	}

	private static Set<String> EMPTY_SET = new HashSet<String>();

	@Override
	public void lock(
			Transaction transaction,
			String featureID ) {
		lock(
				transaction,
				featureID,
				transaction == Transaction.AUTO_COMMIT ? EMPTY_SET : transaction.getAuthorizations(),
				1 /* minutes */);
	}

	private void lock(
			Transaction transaction,
			String featureID,
			Set<String> authorizations,
			long expiryInMinutes ) {
		AuthorizedLock lock = transaction == Transaction.AUTO_COMMIT ? null : (AuthorizedLock) transaction
				.getState(this);
		if (lock == null) {
			lock = new AuthorizedLock(
					this,
					authorizations,
					expiryInMinutes);
			if (transaction != Transaction.AUTO_COMMIT) transaction.putState(
					this,
					lock);
		}
		lock(
				lock,
				featureID);
	}

	private void unlock(
			Transaction transaction,
			String featureID,
			Set<String> authorizations,
			long expiryInMinutes ) {
		AuthorizedLock lock = transaction == Transaction.AUTO_COMMIT ? null : (AuthorizedLock) transaction
				.getState(this);
		if (lock == null) {
			lock = new AuthorizedLock(
					this,
					authorizations,
					expiryInMinutes);
			if (transaction != Transaction.AUTO_COMMIT) transaction.putState(
					this,
					lock);
		}
		unlock(
				lock,
				featureID);
	}

	@Override
	public void lockFeatureID(
			String typeName,
			String featureID,
			Transaction transaction,
			FeatureLock featureLock ) {
		Set<String> set = new LinkedHashSet<String>();
		set.add(featureLock.getAuthorization());
		this.lock(
				transaction,
				featureID,
				set,
				featureLock.getDuration());
	}

	@Override
	public void unLockFeatureID(
			String typeName,
			String featureID,
			Transaction transaction,
			FeatureLock featureLock )
			throws IOException {
		Set<String> set = new LinkedHashSet<String>();
		set.add(featureLock.getAuthorization());
		this.unlock(
				transaction,
				featureID,
				set,
				featureLock.getDuration());
	}

	@Override
	public boolean release(
			String authID,
			Transaction transaction )
			throws IOException {
		AuthorizedLock lock = transaction == Transaction.AUTO_COMMIT ? null : (AuthorizedLock) transaction
				.getState(this);
		if (lock == null) lock = new AuthorizedLock(
				this,
				authID,
				1 /* minutes */);
		releaseAll(lock);
		return true;
	}

	@Override
	public boolean refresh(
			String authID,
			Transaction transaction )
			throws IOException {
		AuthorizedLock lock = transaction == Transaction.AUTO_COMMIT ? null : (AuthorizedLock) transaction
				.getState(this);
		if (lock == null) lock = new AuthorizedLock(
				this,
				authID,
				1 /* minutes */);
		resetAll(lock);
		return true;
	}

	/**
	 * If already locked and request lock has proper authorization
	 * {@link AuthorizedLock#isAuthorized(AuthorizedLock)}, then return. If
	 * already locked and request does not have proper authorization, block
	 * until the lock is released or expired. If not already locked, create the
	 * lock.
	 * 
	 * Make sure there is some mechanism for expired locks to be discovered and
	 * released so that clients are not blocked indefinitely.
	 * 
	 * @param lock
	 * @param featureID
	 */

	public abstract void lock(
			AuthorizedLock lock,
			String featureID );

	/**
	 * 
	 * If authorized {@link AuthorizedLock#isAuthorized(AuthorizedLock)}, unlock
	 * the featureID
	 * 
	 * @param lock
	 * @param featureID
	 */
	public abstract void unlock(
			AuthorizedLock lock,
			String featureID );

	/**
	 * Release all locks associated with a transaction or associated
	 * authorizations. Occurs on commit and rollback. Basically, call
	 * {@link TransactionLock#invalidate())} for all authorized locks
	 * {@link AuthorizedLock#isAuthorized(AuthorizedLock)}
	 * 
	 * @param lock
	 */
	public abstract void releaseAll(
			AuthorizedLock lock );

	/**
	 * Reset all locks associated with a transaction. Occurs on commit and
	 * rollback. Basically, call {@link AuthorizedLock#resetExpireTime()} for
	 * all authorized locks {@link AuthorizedLock#isAuthorized(AuthorizedLock)}
	 * 
	 * @param lock
	 */
	public abstract void resetAll(
			AuthorizedLock lock );

}
