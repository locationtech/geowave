package mil.nga.giat.geowave.adapter.vector.plugin.transaction;

import java.io.IOException;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;

/**
 * Represent the Writer's pluggable strategy of a transaction
 * 
 * 
 * @source $URL$
 */

public interface GeoWaveTransaction
{

	/**
	 * Flush in memory records to store for query processing.
	 */
	public void flush()
			throws IOException;

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

	public Map<ByteArrayId, DataStatistics<SimpleFeature>> getDataStatistics();

	public CloseableIterator<SimpleFeature> interweaveTransaction(
			final Integer limit,
			final Filter filter,
			final CloseableIterator<SimpleFeature> it );
}