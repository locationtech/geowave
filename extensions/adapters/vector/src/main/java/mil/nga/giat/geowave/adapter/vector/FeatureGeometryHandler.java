package mil.nga.giat.geowave.adapter.vector;

import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.IndexFieldHandler;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.AttributeDescriptor;

import com.vividsolutions.jts.geom.Geometry;

/**
 * This class handles the internal responsibility of persisting JTS geometry to
 * and from a GeoWave common index field for SimpleFeature data.
 * 
 */
public class FeatureGeometryHandler implements
		IndexFieldHandler<SimpleFeature, GeometryWrapper, Object>
{
	private final FeatureAttributeHandler nativeGeometryHandler;
	private final FieldVisibilityHandler<SimpleFeature, Object> visibilityHandler;

	public FeatureGeometryHandler(
			final AttributeDescriptor geometryAttrDesc ) {
		this(
				geometryAttrDesc,
				null);
	}

	public FeatureGeometryHandler(
			final AttributeDescriptor geometryAttrDesc,
			final FieldVisibilityHandler<SimpleFeature, Object> visibilityHandler ) {
		nativeGeometryHandler = new FeatureAttributeHandler(
				geometryAttrDesc);
		this.visibilityHandler = visibilityHandler;
	}

	@Override
	public ByteArrayId[] getNativeFieldIds() {
		return new ByteArrayId[] {
			nativeGeometryHandler.getFieldId()
		};
	}

	@Override
	public GeometryWrapper toIndexValue(
			final SimpleFeature row ) {
		final Geometry geometry = (Geometry) nativeGeometryHandler.getFieldValue(row);
		byte[] visibility;
		if (visibilityHandler != null) {
			visibility = visibilityHandler.getVisibility(
					row,
					nativeGeometryHandler.getFieldId(),
					geometry);
		}
		else {
			visibility = new byte[] {};
		}
		return new GeometryWrapper(
				geometry,
				visibility);
	}

	@SuppressWarnings("unchecked")
	@Override
	public PersistentValue<Object>[] toNativeValues(
			final GeometryWrapper indexValue ) {
		return new PersistentValue[] {
			new PersistentValue<Geometry>(
					nativeGeometryHandler.getFieldId(),
					indexValue.getGeometry())
		};
	}

}
