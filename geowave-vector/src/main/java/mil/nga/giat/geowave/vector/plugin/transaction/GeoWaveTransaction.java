package mil.nga.giat.geowave.vector.plugin.transaction;

import java.io.IOException;

import mil.nga.giat.geowave.store.CloseableIterator;

import org.opengis.feature.simple.SimpleFeature;

/**
 * Represent the Writer's pluggable strategy of a transaction
 * 
 * 
 * @source $URL$
 */

public interface GeoWaveTransaction
{

	/**
	 * 
	 * @return true if transaction is empty
	 */
	public boolean isEmpty();

	/**
	 * Record a modification to the indicated fid
	 * 
	 * @param fid
	 * @param f
	 *            replacement feature; null to indicate remove
	 */
	public void modify(
			String fid,
			SimpleFeature old,
			SimpleFeature updated )
			throws IOException;

	public void add(
			String fid,
			SimpleFeature f )
			throws IOException;

	public void remove(
			String fid,
			SimpleFeature feature )
			throws IOException;

	public String[] composeAuthorizations();

	public String composeVisibility();

	public CloseableIterator<SimpleFeature> interweaveTransaction(
			CloseableIterator<SimpleFeature> it );
}