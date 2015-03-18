package mil.nga.giat.geowave.vector.plugin.transaction;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.vector.plugin.GeoWaveDataStoreComponents;
import mil.nga.giat.geowave.vector.plugin.lock.LockingManagement;

import org.geotools.data.DataSourceException;
import org.geotools.data.Transaction;

/**
 * Implements the transaction state protocol with Geotools.
 * 
 */
public class GeoWaveTransactionManagementState implements
		GeoWaveTransactionState
{

	private final GeoWaveDataStoreComponents components;
	private final LockingManagement lockingManager;
	private Transaction transaction;
	private final String txID;

	/**
	 * Map of differences by typeName.
	 * 
	 * <p>
	 * Differences are stored as a Map of Feature by fid, and are reset during a
	 * commit() or rollback().
	 * </p>
	 */
	private Map<String, GeoWaveTransactionManagement> typeNameDiff = new HashMap<String, GeoWaveTransactionManagement>();

	public GeoWaveTransactionManagementState(
			GeoWaveDataStoreComponents components,
			Transaction transaction,
			LockingManagement lockingManager )
			throws IOException {
		this.components = components;
		this.transaction = transaction;
		this.lockingManager = lockingManager;
		this.txID = components.getTransaction();
	}

	public synchronized void setTransaction(
			Transaction transaction ) {
		if (transaction != null) {
			// configure
			this.transaction = transaction;
		}
		else {
			this.transaction = null;

			if (typeNameDiff != null) {
				for (Iterator<GeoWaveTransactionManagement> i = typeNameDiff.values().iterator(); i.hasNext();) {
					GeoWaveTransactionManagement diff = (GeoWaveTransactionManagement) i.next();
					diff.clear();
				}

				typeNameDiff.clear();
			}
		}
	}

	@Override
	public synchronized GeoWaveTransactionManagement getGeoWaveTransaction(
			String typeName )
			throws IOException {
		if (!exists(typeName)) {
			throw new RuntimeException(
					typeName + " not defined");
		}

		if (typeNameDiff.containsKey(typeName)) {
			return (GeoWaveTransactionManagement) typeNameDiff.get(typeName);
		}
		else {
			GeoWaveTransactionManagement transX = new GeoWaveTransactionManagement(
					components,
					typeName,
					transaction,
					lockingManager,
					txID);
			typeNameDiff.put(
					typeName,
					transX);

			return transX;
		}
	}

	boolean exists(
			String typeName ) {
		String[] types;
		types = components.getGTstore().getTypeNames();
		Arrays.sort(types);

		return Arrays.binarySearch(
				types,
				typeName) != -1;
	}

	/**
	 * @see org.geotools.data.Transaction.State#addAuthorization(java.lang.String)
	 */
	public synchronized void addAuthorization(
			String AuthID )
			throws IOException {
		// not required
	}

	/**
	 * Will apply differences to store.
	 * 
	 * @see org.geotools.data.Transaction.State#commit()
	 */
	public synchronized void commit()
			throws IOException {

		try {
			for (Iterator<Entry<String, GeoWaveTransactionManagement>> i = typeNameDiff.entrySet().iterator(); i.hasNext();) {
				final Map.Entry<String, GeoWaveTransactionManagement> entry = i.next();

				final String typeName = entry.getKey();
				GeoWaveTransactionManagement diff = entry.getValue();
				applyDiff(
						typeName,
						diff);
				diff.clear();
			}
		}
		finally {
			this.components.releaseTransaction(txID);
		}
	}

	/**
	 * Called by commit() to apply one set of diff
	 * 
	 * <p>
	 * The provided <code> will be modified as the differences are applied,
	 * If the operations are all successful diff will be empty at
	 * the end of this process.
	 * </p>
	 * 
	 * <p>
	 * diff can be used to represent the following operations:
	 * </p>
	 * 
	 * <ul>
	 * <li>
	 * fid|null: represents a fid being removed</li>
	 * 
	 * <li>
	 * fid|feature: where fid exists, represents feature modification</li>
	 * <li>
	 * fid|feature: where fid does not exist, represents feature being modified</li>
	 * </ul>
	 * 
	 * 
	 * @param typeName
	 *            typeName being updated
	 * @param diff
	 *            differences to apply to FeatureWriter
	 * 
	 * @throws IOException
	 *             If the entire diff cannot be writen out
	 * @throws DataSourceException
	 *             If the entire diff cannot be writen out
	 */
	void applyDiff(
			String typeName,
			GeoWaveTransactionManagement diff )
			throws IOException {
		IOException cause = null;
		if (diff.isEmpty()) {
			return;
		}
		try {
			diff.commit();
		}
		catch (IOException e) {
			cause = e;
			throw e;
		}
		catch (RuntimeException e) {
			cause = new IOException(
					e);
			throw e;
		}
		finally {
			try {
				components.getGTstore().getListenerManager().fireChanged(
						typeName,
						transaction,
						true);
				diff.clear();
			}
			catch (RuntimeException e) {
				if (cause != null) {
					e.initCause(cause);
				}
				throw e;
			}
		}
	}

	/**
	 * @see org.geotools.data.Transaction.State#rollback()
	 */
	public synchronized void rollback()
			throws IOException {
		Entry<String, GeoWaveTransactionManagement> entry;

		try {
			for (Iterator<Entry<String, GeoWaveTransactionManagement>> i = typeNameDiff.entrySet().iterator(); i.hasNext();) {
				entry = i.next();

				String typeName = (String) entry.getKey();
				GeoWaveTransactionManagement diff = (GeoWaveTransactionManagement) entry.getValue();
				diff.rollback();

				components.getGTstore().getListenerManager().fireChanged(
						typeName,
						transaction,
						false);
			}
		}
		finally {
			this.components.releaseTransaction(txID);
		}
	}

}
