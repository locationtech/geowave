package mil.nga.giat.geowave.adapter.vector;

import mil.nga.giat.geowave.adapter.vector.utils.TimeDescriptors;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatisticsProvider;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public interface GeotoolsFeatureDataAdapter extends
		WritableDataAdapter<SimpleFeature>,
		StatisticsProvider<SimpleFeature>
{
	public SimpleFeatureType getFeatureType();

	public TimeDescriptors getTimeDescriptors();

	public boolean hasTemporalConstraints();
}
