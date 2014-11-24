package mil.nga.giat.geowave.analytics.tools;

import java.util.List;

import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;

import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;

/**
 * A set of utilities to describe and create a simple feature for use within the
 * set of analytics.
 * 
 */
public class AnalyticFeature
{
	final static Logger LOGGER = LoggerFactory.getLogger(AnalyticFeature.class);

	public static SimpleFeature createGeometryFeature(
			final SimpleFeatureType featureType,
			final String batchId,
			final String dataId,
			final String name,
			final String groupID,
			final double weight,
			final Geometry geometry,
			final String[] extraDimensionNames,
			final double[] extraDimensions,
			final int zoomLevel,
			final int iteration,
			final long count ) {
		assert (extraDimensionNames.length == extraDimensions.length);
		final List<AttributeDescriptor> descriptors = featureType.getAttributeDescriptors();
		final Object[] defaults = new Object[descriptors.size()];
		int p = 0;
		for (final AttributeDescriptor descriptor : descriptors) {
			defaults[p++] = descriptor.getDefaultValue();
		}

		final SimpleFeature newFeature = SimpleFeatureBuilder.build(
				featureType,
				defaults,
				dataId);
		newFeature.setAttribute(
				ClusterFeatureAttribute.Name.attrName(),
				name);
		newFeature.setAttribute(
				ClusterFeatureAttribute.GroupID.attrName(),
				groupID);
		newFeature.setAttribute(
				ClusterFeatureAttribute.Iteration.attrName(),
				iteration);
		newFeature.setAttribute(
				ClusterFeatureAttribute.Weight.attrName(),
				weight);
		newFeature.setAttribute(
				ClusterFeatureAttribute.BatchId.attrName(),
				batchId);
		newFeature.setAttribute(
				ClusterFeatureAttribute.Count.attrName(),
				count);
		newFeature.setAttribute(
				ClusterFeatureAttribute.Geometry.attrName(),
				geometry);
		newFeature.setAttribute(
				ClusterFeatureAttribute.ZoomLevel.attrName(),
				zoomLevel);
		int i = 0;
		for (final String dimName : extraDimensionNames) {
			newFeature.setAttribute(
					dimName,
					new Double(
							extraDimensions[i++]));
		}
		return newFeature;
	}

	public static FeatureDataAdapter createGeometryFeatureAdapter(
			final String centroidDataTypeId,
			final String[] extraNumericDimensions,
			final int SRID ) {
		try {
			final StringBuffer buffer = new StringBuffer();
			for (final ClusterFeatureAttribute attrVal : ClusterFeatureAttribute.values()) {

				buffer.append(attrVal.name);
				buffer.append(':');
				if (attrVal == ClusterFeatureAttribute.Geometry) {
					buffer.append("Geometry:srid=");
					buffer.append(SRID);
				}
				else {
					buffer.append(attrVal.type);
				}
				buffer.append(',');
			}
			for (final String extraDim : extraNumericDimensions) {
				buffer.append(
						extraDim).append(
						":java.lang.Double").append(
						',');
			}
			buffer.deleteCharAt(buffer.length() - 1);
			return new FeatureDataAdapter(
					DataUtilities.createType(
							centroidDataTypeId, // column family in GeoWave key
							buffer.toString()));
		}
		catch (final SchemaException e) {
			LOGGER.warn(
					"Schema Creation Error.  Hint: Check the SRID.",
					e);
		}

		return null;
	}

	public static enum ClusterFeatureAttribute {
		Name(
				"name",
				"String"),
		GroupID(
				"groupID",
				"String"),
		Iteration(
				"iteration",
				"Integer"),
		Geometry(
				"geometry",
				"Geometry:srid=4326"),
		Weight(
				"weight",
				"Double"),
		Count(
				"count",
				"java.lang.Long"),
		ZoomLevel(
				"level",
				"Integer"),
		BatchId(
				"batchID",
				"String");

		private final String name;
		private final String type;

		ClusterFeatureAttribute(
				final String name,
				final String type ) {
			this.name = name;
			this.type = type;
		}

		public String attrName() {
			return name;
		}

		public String getType() {
			return type;
		}
	}

}
