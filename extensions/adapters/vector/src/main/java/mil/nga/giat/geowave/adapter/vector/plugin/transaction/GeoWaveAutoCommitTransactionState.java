package mil.nga.giat.geowave.adapter.vector.plugin.transaction;

import java.io.IOException;

import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveDataStoreComponents;
import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveFeatureSource;

import org.geotools.data.Transaction;

public class GeoWaveAutoCommitTransactionState implements
		GeoWaveTransactionState
{

	private final GeoWaveDataStoreComponents components;

	public GeoWaveAutoCommitTransactionState(
			GeoWaveFeatureSource source ) {
		this.components = source.getComponents();
	}

	public void setTransaction(
			Transaction transaction ) {}

	/**
	 * @see org.geotools.data.Transaction.State#addAuthorization(java.lang.String)
	 */
	@Override
	public void addAuthorization(
			String AuthID )
			throws IOException {
		// not required for
	}

	/**
	 * Will apply differences to store.
	 * 
	 * @see org.geotools.data.Transaction.State#commit()
	 */
	@Override
	public void commit()
			throws IOException {
		// not required for
	}

	/**
	 * @see org.geotools.data.Transaction.State#rollback()
	 */
	public void rollback()
			throws IOException {

	}

	@Override
	public GeoWaveTransaction getGeoWaveTransaction(
			String typeName ) {
		// TODO Auto-generated method stub
		return new GeoWaveEmptyTransaction(
				components);
	}

}
