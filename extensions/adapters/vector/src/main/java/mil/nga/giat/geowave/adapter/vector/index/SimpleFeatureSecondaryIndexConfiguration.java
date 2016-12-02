package mil.nga.giat.geowave.adapter.vector.index;

import java.util.Set;

import org.codehaus.jackson.annotate.JsonIgnore;

import mil.nga.giat.geowave.adapter.vector.utils.SimpleFeatureUserDataConfiguration;

public interface SimpleFeatureSecondaryIndexConfiguration extends
		SimpleFeatureUserDataConfiguration
{
	@JsonIgnore
	public String getIndexKey();

	public Set<String> getAttributes();
}
