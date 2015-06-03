package mil.nga.giat.geowave.adapter.vector.utils;

import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.opengis.feature.simple.SimpleFeatureType;

/**
 * 
 * A type of configuration data associated with attributes of a simple features
 * such as statistics, indexing constraints, etc.
 * 
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public interface SimpleFeatureUserDataConfiguration extends
		java.io.Serializable
{
	/**
	 * Store configuration in user data of the feature type attributes.
	 * 
	 * @param type
	 */
	public void updateType(
			final SimpleFeatureType type );

	/**
	 * Extract configuration from user data of the feature type attributes.
	 * 
	 * @param type
	 */
	public void configureFromType(
			final SimpleFeatureType type );
}
