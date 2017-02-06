package mil.nga.giat.geowave.adapter.vector.plugin.transaction;

import java.io.IOException;

import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveDataStoreComponents;
import mil.nga.giat.geowave.core.store.CloseableIterator;

import org.geotools.data.Transaction;
import org.geotools.factory.Hints;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;

/**
 * Commit changes immediately
 * 
 * @source $URL$
 */

public class GeoWaveEmptyTransaction extends
		AbstractTransactionManagement implements
		GeoWaveTransaction
{

	/** Create an empty Diff */
	public GeoWaveEmptyTransaction(
			GeoWaveDataStoreComponents components ) {
		super(
				components);
	}

	/**
	 * Return true if transaction is empty
	 */
	public boolean isEmpty() {
		return true;
	}

	@Override
	public void flush()
			throws IOException {

	}

	/**
	 * Record a modification to the indicated fid
	 * 
	 * @param fid
	 * @param original
	 *            the original feature(prior state)
	 * @param updated
	 *            the update feature replacement feature; null to indicate
	 *            remove
	 */
	public void modify(
			String fid,
			SimpleFeature original,
			SimpleFeature updated )
			throws IOException {
		// point move?
		if (!updated.getBounds().equals(
				original.getBounds())) {
			this.components.remove(
					original,
					this);
			this.components.writeCommit(
					updated,
					new GeoWaveEmptyTransaction(
							components));
		}
		else {
			this.components.writeCommit(
					updated,
					new GeoWaveEmptyTransaction(
							components));
		}

		ReferencedEnvelope bounds = new ReferencedEnvelope();
		bounds.include(updated.getBounds());
		bounds.include(original.getBounds());
		this.components.getGTstore().getListenerManager().fireFeaturesChanged(
				updated.getFeatureType().getTypeName(),
				Transaction.AUTO_COMMIT,
				bounds,
				true);

	}

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
		this.components.writeCommit(
				feature,
				this);

		components.getGTstore().getListenerManager().fireFeaturesAdded(
				components.getAdapter().getFeatureType().getTypeName(),
				Transaction.AUTO_COMMIT,
				ReferencedEnvelope.reference(feature.getBounds()),
				true);
	}

	public void remove(
			String fid,
			SimpleFeature feature )
			throws IOException {
		this.components.remove(
				feature,
				this);
		this.components.getGTstore().getListenerManager().fireFeaturesRemoved(
				feature.getFeatureType().getTypeName(),
				Transaction.AUTO_COMMIT,
				ReferencedEnvelope.reference(feature.getBounds()),
				true);
	}

	public String getID() {
		return "";
	}

	public CloseableIterator<SimpleFeature> interweaveTransaction(
			final Integer limit,
			final Filter filter,
			final CloseableIterator<SimpleFeature> it ) {
		return it;
	}

	@Override
	public String[] composeAuthorizations() {
		return this.components.getGTstore().getAuthorizationSPI().getAuthorizations();

	}

	@Override
	public String composeVisibility() {
		return "";
	}

}