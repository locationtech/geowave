package mil.nga.giat.geowave.adapter.vector.plugin;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;

import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.BasicQuery;

public interface QueryIssuer
{
	CloseableIterator<SimpleFeature> query(
			PrimaryIndex index,
			BasicQuery constraints );

	Filter getFilter();

	Integer getLimit();

}
