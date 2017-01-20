package mil.nga.giat.geowave.adapter.vector.plugin.lock;

import org.geotools.data.LockingManager;
import org.geotools.data.Transaction;

/**
 * An extension to {@link LockManager} to support requesting a lock on a
 * specific feature under a provided transaction. Implementers must check
 * transaction state as AUTO_COMMIT. Locking under an AUTO_COMMIT is not
 * authorized.
 * 
 * 
 * 
 */
public interface LockingManagement extends
		LockingManager
{

	/**
	 * Lock a feature for a provided transaction. This is typically used for
	 * modifications (updates).
	 * 
	 * @param transaction
	 * @param featureID
	 */
	public void lock(
			Transaction transaction,
			String featureID );
}
