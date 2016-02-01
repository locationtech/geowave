package mil.nga.giat.geowave.adapter.vector.index;

import java.util.Map;

import org.opengis.feature.simple.SimpleFeature;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.query.BasicQuery;

public interface IndexQueryStrategySPI
{
	public CloseableIterator<Index<?, ?>> getIndices(
			Map<ByteArrayId, DataStatistics<SimpleFeature>> stats,
			BasicQuery query,
			CloseableIterator<Index<?, ?>> indices );
}
