package mil.nga.giat.geowave.vector.plugin.transaction;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import mil.nga.giat.geowave.accumulo.util.VisibilityTransformer;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.vector.plugin.GeoWaveDataStoreComponents;
import mil.nga.giat.geowave.vector.plugin.lock.LockingManagement;

import org.apache.commons.lang3.tuple.Pair;
import org.geotools.data.Transaction;
import org.geotools.factory.Hints;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

/**
 * Captures changes made to a FeatureStore prior to being committed.
 * <p>
 * This is used to simulate the functionality of a database including
 * transaction independence.
 * 
 * 
 * @source $URL$
 */

public class GeoWaveTransactionManagement implements
		GeoWaveTransaction
{
	/** Map of modified features; by feature id */
	private final Map<String, ModifiedFeature> modifiedFeatures = new ConcurrentHashMap<String, ModifiedFeature>();
	private final Multimap<String, SimpleFeature> removedFeatures = LinkedListMultimap.create();

	/** List of added feature ids; values stored in added above */
	private final Map<String, List<ByteArrayId>> addedFidList = new HashMap<String, List<ByteArrayId>>();

	private final GeoWaveDataStoreComponents components;

	private final LockingManagement lockingManager;

	private final Transaction transaction;

	private final String txID;

	private final String typeName;

	private static class ModifiedFeature
	{
		public ModifiedFeature(
				final SimpleFeature oldFeature,
				final SimpleFeature newFeature,
				boolean alreadyWritten ) {
			super();
			this.newFeature = newFeature;
			this.oldFeature = oldFeature;
			this.alreadyWritten = alreadyWritten;
		}

		final boolean alreadyWritten;
		final SimpleFeature newFeature;
		final SimpleFeature oldFeature;

	}

	/** Simple object used for locking */
	final Object mutex;

	/**
	 * Create an empty Diff
	 * 
	 * @throws IOException
	 */
	public GeoWaveTransactionManagement(
			final GeoWaveDataStoreComponents components,
			final String typeName,
			final Transaction transaction,
			LockingManagement lockingManager,
			String txID )
			throws IOException {
		this.components = components;
		mutex = this;
		this.typeName = typeName;
		this.transaction = transaction;
		this.lockingManager = lockingManager;
		this.txID = txID;
	}

	/**
	 * Check if modifiedFeatures and addedFeatures are empty.
	 * 
	 * @return true if Diff is empty
	 */
	@Override
	public boolean isEmpty() {
		synchronized (mutex) {
			return modifiedFeatures.isEmpty() && addedFidList.isEmpty() && this.removedFeatures.isEmpty();
		}
	}

	/**
	 * Clear diff - called during rollback.
	 */
	public void clear() {
		synchronized (mutex) {
			addedFidList.clear();
			modifiedFeatures.clear();
			removedFeatures.clear();
		}
	}

	/**
	 * Record a modification to the indicated fid
	 * 
	 * @param fid
	 * @param f
	 *            replacement feature; null to indicate remove
	 */
	@Override
	public void modify(
			String fid,
			SimpleFeature original,
			SimpleFeature updated )
			throws IOException {

		lockingManager.lock(
				transaction,
				fid);
		// assumptions: (1) will not get a modification to a deleted feature
		// thus, only contents of the removed features collection for this
		// feature relate to moving bounds.
		// @see {@link #interweaveTransaction(CloseableIterator<SimpleFeature>)}
		//
		// Cannot assume that a modification occurs for a newly added fid

		// TODO: skipping this for now. creates a problem because
		// the row IDs may or maynot change. If they change completely, then
		// it is not an issue. However, a mix of changed or unchanged means
		// that the original rows become invisible for the duration of the
		// transaction

		// The problem now is that the bounded query may not return the moved
		// record, if it has moved outside
		// the query space. oh well!

		ModifiedFeature modRecord = modifiedFeatures.get(fid);

		if (!updated.getBounds().equals(
				original.getBounds())) {

			// retain original--original position is removed later.
			// The original feature needs to be excluded in a query
			// and removed at commit
			this.removedFeatures.put(
					fid,
					original);

		}
		if ((modRecord != null && modRecord.alreadyWritten) || addedFidList.containsKey(fid)) {
			this.components.writeCommit(
					updated,
					this);
			synchronized (mutex) {
				modifiedFeatures.put(
						fid,
						new ModifiedFeature(
								modRecord.oldFeature,
								updated,
								true));
			}
		}
		else {
			synchronized (mutex) {
				modifiedFeatures.put(
						fid,
						new ModifiedFeature(
								modRecord == null ? original : modRecord.oldFeature,
								updated,
								false));
			}
		}
		ReferencedEnvelope bounds = new ReferencedEnvelope(
				(CoordinateReferenceSystem) null);
		bounds.include(original.getBounds());
		bounds.include(updated.getBounds());
		components.getGTstore().getListenerManager().fireFeaturesChanged(
				components.getAdapter().getType().getTypeName(),
				transaction,
				bounds,
				false);

	}

	@Override
	public void add(
			String fid,
			SimpleFeature feature )
			throws IOException {
		feature.getUserData().put(
				Hints.USE_PROVIDED_FID,
				true);
		if (feature.getUserData().containsKey(
				Hints.PROVIDED_FID)) {
			String providedFid = (String) feature.getUserData().get(
					Hints.PROVIDED_FID);
			feature.getUserData().put(
					Hints.PROVIDED_FID,
					providedFid);
		}
		else {
			feature.getUserData().put(
					Hints.PROVIDED_FID,
					feature.getID());
		}
		List<ByteArrayId> rowIDS = this.components.write(
				feature,
				this);
		synchronized (mutex) {
			addedFidList.put(
					fid,
					rowIDS); // preserve order features are added
								// in
		}
		components.getGTstore().getListenerManager().fireFeaturesAdded(
				components.getAdapter().getType().getTypeName(),
				transaction,
				ReferencedEnvelope.reference(feature.getBounds()),
				false);
	}

	@Override
	public void remove(
			String fid,
			SimpleFeature feature )
			throws IOException {
		synchronized (mutex) {
			if (addedFidList.remove(fid) != null)
				this.components.remove(
						feature,
						this);
			else {
				// will remove at the end of the transaction, except ones
				// created in the transaction.
				removedFeatures.put(
						fid,
						feature);
				modifiedFeatures.remove(fid);
			}
		}
		components.getGTstore().getListenerManager().fireFeaturesRemoved(
				components.getAdapter().getType().getTypeName(),
				transaction,
				ReferencedEnvelope.reference(feature.getBounds()),
				false);
	}

	public void rollback()
			throws IOException {
		for (String fid : this.addedFidList.keySet()) {
			components.remove(
					fid,
					this);
		}
		this.clear();
	}

	@Override
	public String[] composeAuthorizations() {
		final String[] initialAuths = this.components.getGTstore().getAuthorizationSPI().getAuthorizations();
		final String[] newAuths = new String[initialAuths.length + 1];
		System.arraycopy(
				initialAuths,
				0,
				newAuths,
				0,
				initialAuths.length);
		newAuths[initialAuths.length] = this.txID;
		return newAuths;
	}

	@Override
	public String composeVisibility() {
		return this.txID;
	}

	public String getID() {
		return txID;
	}

	public void commit()
			throws IOException {
		final Iterator<Pair<SimpleFeature, SimpleFeature>> updateIt = getUpdates();

		final String transId = "\\(?" + txID + "\\)?";
		VisibilityTransformer visibilityTransformer = new VisibilityTransformer(
				"&?" + transId,
				"");
		for (Collection<ByteArrayId> rowIDs : this.addedFidList.values()) {
			this.components.replaceDataVisibility(
					this,
					rowIDs,
					visibilityTransformer);
		}

		this.components.replaceStatsVisibility(
				this,
				visibilityTransformer);

		final Iterator<SimpleFeature> removeIt = this.removedFeatures.values().iterator();

		while (removeIt.hasNext()) {
			final SimpleFeature delFeatured = removeIt.next();
			components.remove(
					delFeatured,
					this);
			ModifiedFeature modFeature = this.modifiedFeatures.get(delFeatured.getID());
			// only want notify updates to existing (not new) features
			if (modFeature == null || modFeature.alreadyWritten) components.getGTstore().getListenerManager().fireFeaturesRemoved(
					typeName,
					transaction,
					ReferencedEnvelope.reference(delFeatured.getBounds()),
					true);
		}

		while (updateIt.hasNext()) {
			final Pair<SimpleFeature, SimpleFeature> pair = updateIt.next();
			components.writeCommit(
					pair.getRight(),
					this);
			ReferencedEnvelope bounds = new ReferencedEnvelope(
					(CoordinateReferenceSystem) null);
			bounds.include(pair.getLeft().getBounds());
			bounds.include(pair.getRight().getBounds());
			components.getGTstore().getListenerManager().fireFeaturesChanged(
					typeName,
					transaction,
					ReferencedEnvelope.reference(pair.getRight().getBounds()),
					true);
		}

	}

	private Iterator<Pair<SimpleFeature, SimpleFeature>> getUpdates() {
		final Iterator<Entry<String, ModifiedFeature>> entries = this.modifiedFeatures.entrySet().iterator();
		return new Iterator<Pair<SimpleFeature, SimpleFeature>>() {

			Pair<SimpleFeature, SimpleFeature> pair = null;

			@Override
			public boolean hasNext() {
				while (entries.hasNext() && pair == null) {
					Entry<String, ModifiedFeature> entry = entries.next();
					if (!entry.getValue().alreadyWritten)
						pair = Pair.of(
								entry.getValue().oldFeature,
								entry.getValue().newFeature);
					else
						pair = null;
				}
				return pair != null;
			}

			@Override
			public Pair<SimpleFeature, SimpleFeature> next()
					throws NoSuchElementException {
				if (pair == null) throw new NoSuchElementException();
				final Pair<SimpleFeature, SimpleFeature> retVal = pair;
				pair = null;
				return retVal;
			}

			@Override
			public void remove() {}
		};
	}

	@Override
	public CloseableIterator<SimpleFeature> interweaveTransaction(
			final CloseableIterator<SimpleFeature> it ) {
		return new CloseableIterator<SimpleFeature>() {

			SimpleFeature feature = null;

			@Override
			public boolean hasNext() {
				while (it.hasNext() && feature == null) {
					feature = it.next();
					ModifiedFeature modRecord = GeoWaveTransactionManagement.this.modifiedFeatures.get(feature.getID());
					// exclude removed features
					// and include updated features not written yet.
					Collection<SimpleFeature> oldFeatures = GeoWaveTransactionManagement.this.removedFeatures.get(feature.getID());

					if (modRecord != null)
						feature = modRecord.newFeature;
					else if (oldFeatures != null && !oldFeatures.isEmpty()) // TODO:
																			// need
																			// to
																			// check
																			// if
																			// the
																			// removed
																			// feature
																			// was
																			// just
																			// moved
						// meaning its original matches the boundaries of this
						// 'feature'. matchesOne(oldFeatures, feature))
						feature = null;

				}
				return feature != null;
			}

			@Override
			public SimpleFeature next()
					throws NoSuchElementException {
				if (feature == null) throw new NoSuchElementException();
				final SimpleFeature retVal = feature;
				feature = null;
				return retVal;
			}

			@Override
			public void remove() {
				GeoWaveTransactionManagement.this.removedFeatures.put(
						feature.getID(),
						feature);
			}

			@Override
			public void close()
					throws IOException {
				it.close();
			}

		};
	}

}