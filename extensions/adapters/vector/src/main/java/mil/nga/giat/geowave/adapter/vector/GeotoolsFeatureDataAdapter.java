package mil.nga.giat.geowave.adapter.vector;

import mil.nga.giat.geowave.adapter.vector.utils.TimeDescriptors;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatisticalDataAdapter;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public interface GeotoolsFeatureDataAdapter extends
		WritableDataAdapter<SimpleFeature>,
		StatisticalDataAdapter<SimpleFeature>
{
	public SimpleFeatureType getType();

	public TimeDescriptors getTimeDescriptors();

	public boolean hasTemporalConstraints();
}
