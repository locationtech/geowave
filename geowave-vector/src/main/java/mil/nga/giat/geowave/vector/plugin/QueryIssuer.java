package mil.nga.giat.geowave.vector.plugin;

import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.query.Query;

import org.opengis.feature.simple.SimpleFeature;

public interface QueryIssuer
{
	CloseableIterator<SimpleFeature> query(
			Index index, Query constraints );
}
