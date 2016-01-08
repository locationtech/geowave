package mil.nga.giat.geowave.adapter.vector;

import mil.nga.giat.geowave.adapter.vector.utils.TimeDescriptors;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatisticalDataAdapter;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.visibility.VisibilityManagement;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public interface GeotoolsFeatureDataAdapter extends
		WritableDataAdapter<SimpleFeature>,
		StatisticalDataAdapter<SimpleFeature>
{
	public SimpleFeatureType getType();

	public TimeDescriptors getTimeDescriptors();

	public String getVisibilityAttributeName();

	public VisibilityManagement<SimpleFeature> getFieldVisibilityManagement();

	public FieldVisibilityHandler<SimpleFeature, Object> getFieldVisiblityHandler();
}
