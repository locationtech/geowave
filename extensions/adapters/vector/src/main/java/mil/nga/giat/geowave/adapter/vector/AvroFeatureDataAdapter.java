package mil.nga.giat.geowave.adapter.vector;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.adapter.vector.plugin.visibility.JsonDefinitionColumnVisibilityManagement;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import mil.nga.giat.geowave.core.store.adapter.PersistentIndexFieldHandler;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.data.visibility.VisibilityManagement;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

/**
 */
public class AvroFeatureDataAdapter extends
		FeatureDataAdapter
{

	protected AvroFeatureDataAdapter() {}

	public AvroFeatureDataAdapter(
			final SimpleFeatureType type ) {
		super(
				type,
				new ArrayList<PersistentIndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>>());
	}

	public AvroFeatureDataAdapter(
			final SimpleFeatureType type,
			final VisibilityManagement<SimpleFeature> visibilityManagement ) {
		super(
				type,
				new ArrayList<PersistentIndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>>(),
				null,
				visibilityManagement);
	}

	public AvroFeatureDataAdapter(
			final SimpleFeatureType type,
			final List<PersistentIndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>> customIndexHandlers ) {
		super(
				type,
				customIndexHandlers);
	}

	public AvroFeatureDataAdapter(
			final SimpleFeatureType type,
			final FieldVisibilityHandler<SimpleFeature, Object> fieldVisiblityHandler ) {
		super(
				type,
				fieldVisiblityHandler);
	}

	@Override
	protected List<NativeFieldHandler<SimpleFeature, Object>> getFieldHandlersFromFeatureType(
			final SimpleFeatureType type ) {
		final List<NativeFieldHandler<SimpleFeature, Object>> nativeHandlers = new ArrayList<NativeFieldHandler<SimpleFeature, Object>>(
				1);

		nativeHandlers.add(new AvroFeatureAttributeHandler());
		return nativeHandlers;
	}

	@Override
	public FieldReader<Object> getReader(
			final ByteArrayId fieldId ) {
		return (FieldReader<Object>) new AvroFeatureReader();
	}

	@Override
	public FieldWriter<SimpleFeature, Object> getWriter(
			final ByteArrayId fieldId ) {
		return (FieldWriter<SimpleFeature, Object>) new AvroFeatureWriter();
	}

	@Override
	protected RowBuilder<SimpleFeature, Object> newBuilder() {
		return new AvroAttributeRowBuilder();
	}
}