package mil.nga.giat.geowave.adapter.vector;

import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.IndexFieldHandler;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.dimension.ArrayWrapper;

import org.geotools.feature.DefaultFeatureCollection;
import org.opengis.feature.type.AttributeDescriptor;

import com.vividsolutions.jts.geom.Geometry;

/**
 * This class handles the internal responsibility of persisting JTS geometries
 * to and from a GeoWave common index field for FeatureCollection data.
 * 
 */
public class FeatureCollectionGeometryHandler implements
		IndexFieldHandler<DefaultFeatureCollection, ArrayWrapper<GeometryWrapper>, Object>
{
	private final FeatureCollectionAttributeHandler nativeGeometryHandler;
	private final FieldVisibilityHandler<DefaultFeatureCollection, Object> visibilityHandler;

	public FeatureCollectionGeometryHandler(
			final AttributeDescriptor geometryAttrDesc ) {
		this(
				geometryAttrDesc,
				null);
	}

	public FeatureCollectionGeometryHandler(
			final AttributeDescriptor geometryAttrDesc,
			final FieldVisibilityHandler<DefaultFeatureCollection, Object> visibilityHandler ) {
		nativeGeometryHandler = new FeatureCollectionAttributeHandler(
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
	public ArrayWrapper<GeometryWrapper> toIndexValue(
			final DefaultFeatureCollection row ) {
		final Geometry[] geometry = (Geometry[]) nativeGeometryHandler.getFieldValue(row);
		final GeometryWrapper[] geometryWrappers = new GeometryWrapper[geometry.length];
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
		for (int i = 0; i < geometry.length; i++) {
			geometryWrappers[i] = new GeometryWrapper(
					geometry[i],
					visibility);
		}
		return new ArrayWrapper<GeometryWrapper>(
				geometryWrappers,
				visibility);
	}

	@SuppressWarnings("unchecked")
	@Override
	public PersistentValue<Object>[] toNativeValues(
			final ArrayWrapper<GeometryWrapper> indexValue ) {
		final GeometryWrapper[] geometryWrappers = indexValue.getArray();
		final Geometry[] geometry = new Geometry[geometryWrappers.length];
		for (int i = 0; i < geometryWrappers.length; i++) {
			geometry[i] = geometryWrappers[i].getGeometry();
		}
		return new PersistentValue[] {
			new PersistentValue<Geometry[]>(
					nativeGeometryHandler.getFieldId(),
					geometry)
		};
	}
}
