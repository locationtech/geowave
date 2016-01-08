package mil.nga.giat.geowave.adapter.vector.index;

import mil.nga.giat.geowave.adapter.vector.utils.SimpleFeatureUserDataConfiguration;

public interface SimpleFeatureSecondaryIndexConfiguration extends
		SimpleFeatureUserDataConfiguration
{
	public String getIndexKey();
}
