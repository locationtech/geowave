package mil.nga.giat.geowave.adapter.vector.plugin.transaction;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveDataStoreComponents;
import mil.nga.giat.geowave.adapter.vector.plugin.lock.LockingManagement;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.datastore.accumulo.util.VisibilityTransformer;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
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

	private static final Logger LOGGER = Logger.getLogger(GeoWaveTransactionManagement.class);

	/** Map of modified features; by feature id */
	private final Map<String, ModifiedFeature> modifiedFeatures = new ConcurrentHashMap<String, ModifiedFeature>();
	private final Map<String, SimpleFeature> addedFeatures = new ConcurrentHashMap<String, SimpleFeature>();
	private final Multimap<String, SimpleFeature> removedFeatures = LinkedListMultimap.create();

	/** List of added feature ids; values stored in added above */
	private final Map<String, List<ByteArrayId>> addedFidList = new HashMap<String, List<ByteArrayId>>();

	private final GeoWaveDataStoreComponents components;
	private int maxAdditionBufferSize = 10000;

	private final LockingManagement lockingManager;

	private final Transaction transaction;

	private final String txID;

	private final String typeName;

	private static class ModifiedFeature
	{
		public ModifiedFeature(
				final SimpleFeature oldFeature,
				final SimpleFeature newFeature,
				final boolean alreadyWritten ) {
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
			final int maxAdditionBufferSize,
			final GeoWaveDataStoreComponents components,
			final String typeName,
			final Transaction transaction,
			final LockingManagement lockingManager,
			final String txID )
			throws IOException {
		this.maxAdditionBufferSize = maxAdditionBufferSize;
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
			return modifiedFeatures.isEmpty() && addedFidList.isEmpty() && removedFeatures.isEmpty() && addedFeatures.isEmpty();
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
			addedFeatures.clear();
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
			final String fid,
			final SimpleFeature original,
			final SimpleFeature updated )
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

		final ModifiedFeature modRecord = modifiedFeatures.get(fid);

		if (!updated.getBounds().equals(
				original.getBounds())) {

			// retain original--original position is removed later.
			// The original feature needs to be excluded in a query
			// and removed at commit
			removedFeatures.put(
					fid,
					original);

		}
		if (((modRecord != null) && modRecord.alreadyWritten) || addedFidList.containsKey(fid)) {
			components.writeCommit(
					updated,
					this);
			synchronized (mutex) {
				if (modRecord != null) {
					modifiedFeatures.put(
							fid,
							new ModifiedFeature(
									modRecord.oldFeature,
									updated,
									true));
				}
				else {
					LOGGER.error("modRecord was set to null in another thread; synchronization issue");
				}
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
		final ReferencedEnvelope bounds = new ReferencedEnvelope(
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
			final String fid,
			final SimpleFeature feature )
			throws IOException {
		feature.getUserData().put(
				Hints.USE_PROVIDED_FID,
				true);
		if (feature.getUserData().containsKey(
				Hints.PROVIDED_FID)) {
			final String providedFid = (String) feature.getUserData().get(
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
		if (addedFeatures.size() == maxAdditionBufferSize) {
			flushAddsToStore(false);
		}
		addedFeatures.put(
				fid,
				feature);
		components.getGTstore().getListenerManager().fireFeaturesAdded(
				components.getAdapter().getType().getTypeName(),
				transaction,
				ReferencedEnvelope.reference(feature.getBounds()),
				false);
	}

	@Override
	public void remove(
			final String fid,
			final SimpleFeature feature )
			throws IOException {
		synchronized (mutex) {
			if (addedFidList.remove(fid) != null) {
				components.remove(
						feature,
						this);
			}
			else {
				addedFeatures.remove(fid);
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
		for (final String fid : addedFidList.keySet()) {
			components.remove(
					fid,
					this);
		}
		clear();
	}

	@Override
	public String[] composeAuthorizations() {
		final String[] initialAuths = components.getGTstore().getAuthorizationSPI().getAuthorizations();
		final String[] newAuths = new String[initialAuths.length + 1];
		System.arraycopy(
				initialAuths,
				0,
				newAuths,
				0,
				initialAuths.length);
		newAuths[initialAuths.length] = txID;
		return newAuths;
	}

	@Override
	public String composeVisibility() {
		return txID;
	}

	public String getID() {
		return txID;
	}

	@Override
	public void flush()
			throws IOException {
		flushAddsToStore(false);
	}

	private void flushAddsToStore(
			final boolean autoCommitAdds )
			throws IOException {
		final Map<String, List<ByteArrayId>> captureList = autoCommitAdds ? new HashMap<String, List<ByteArrayId>>() : addedFidList;
		components.write(
				addedFeatures.values().iterator(),
				captureList,
				autoCommitAdds ? new GeoWaveEmptyTransaction(
						components) : this);
		addedFeatures.clear();
	}

	public void commit()
			throws IOException {

		flushAddsToStore(true);

		final Iterator<Pair<SimpleFeature, SimpleFeature>> updateIt = getUpdates();

		if (addedFidList.size() > 0) {
			final String transId = "\\(?" + txID + "\\)?";
			final VisibilityTransformer visibilityTransformer = new VisibilityTransformer(
					"&?" + transId,
					"");
			for (final Collection<ByteArrayId> rowIDs : addedFidList.values()) {
				components.replaceDataVisibility(
						this,
						rowIDs,
						visibilityTransformer);
			}

			components.replaceStatsVisibility(
					this,
					visibilityTransformer);
		}

		final Iterator<SimpleFeature> removeIt = removedFeatures.values().iterator();

		while (removeIt.hasNext()) {
			final SimpleFeature delFeatured = removeIt.next();
			components.remove(
					delFeatured,
					this);
			final ModifiedFeature modFeature = modifiedFeatures.get(delFeatured.getID());
			// only want notify updates to existing (not new) features
			if ((modFeature == null) || modFeature.alreadyWritten) {
				components.getGTstore().getListenerManager().fireFeaturesRemoved(
						typeName,
						transaction,
						ReferencedEnvelope.reference(delFeatured.getBounds()),
						true);
			}
		}

		while (updateIt.hasNext()) {
			final Pair<SimpleFeature, SimpleFeature> pair = updateIt.next();
			components.writeCommit(
					pair.getRight(),
					this);
			final ReferencedEnvelope bounds = new ReferencedEnvelope(
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
		final Iterator<Entry<String, ModifiedFeature>> entries = modifiedFeatures.entrySet().iterator();
		return new Iterator<Pair<SimpleFeature, SimpleFeature>>() {

			Pair<SimpleFeature, SimpleFeature> pair = null;

			@Override
			public boolean hasNext() {
				while (entries.hasNext() && (pair == null)) {
					final Entry<String, ModifiedFeature> entry = entries.next();
					if (!entry.getValue().alreadyWritten) {
						pair = Pair.of(
								entry.getValue().oldFeature,
								entry.getValue().newFeature);
					}
					else {
						pair = null;
					}
				}
				return pair != null;
			}

			@Override
			public Pair<SimpleFeature, SimpleFeature> next()
					throws NoSuchElementException {
				if (pair == null) {
					throw new NoSuchElementException();
				}
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
				while (it.hasNext() && (feature == null)) {
					feature = it.next();
					final ModifiedFeature modRecord = modifiedFeatures.get(feature.getID());
					// exclude removed features
					// and include updated features not written yet.
					final Collection<SimpleFeature> oldFeatures = removedFeatures.get(feature.getID());

					if (modRecord != null) {
						feature = modRecord.newFeature;
					}
					else if ((oldFeatures != null) && !oldFeatures.isEmpty()) {
						// need to check if the removed feature
						// was just moved meaning its original matches the
						// boundaries of this 'feature'. matchesOne(oldFeatures,
						// feature))
						feature = null;
					}

				}
				return feature != null;
			}

			@Override
			public SimpleFeature next()
					throws NoSuchElementException {
				if (feature == null) {
					throw new NoSuchElementException();
				}
				final SimpleFeature retVal = feature;
				feature = null;
				return retVal;
			}

			@Override
			public void remove() {
				removedFeatures.put(
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