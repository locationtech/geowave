package mil.nga.giat.geowave.adapter.vector.plugin.visibility;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;

import org.opengis.feature.simple.SimpleFeature;

/**
 * Works with {@link VisibilityManagement} instance to define the visibility for
 * a specific field of a data row object. Performs the same function as
 * {@link FieldLevelVisibiltyHandler}. However, this class is linked directly to
 * a {@link FeatureDataAdapter}. The setup of the handlers in
 * {@link FeatureDataAdapter} occurs prior to the resolution of the
 * {@link VisibilityManagement}. Referencing the {@link FeatureDataAdapter}
 * removes the initialization order constraint.
 * 
 * 
 * 
 * @param <CommonIndexValue>
 */
public class AdaptorProxyFieldLevelVisibilityHandler implements
		FieldVisibilityHandler<SimpleFeature, Object>
{

	private final String fieldName;
	private final FeatureDataAdapter adapter;
	private FieldVisibilityHandler<SimpleFeature, Object> myDeferredHandler = null;

	/**
	 * Used when acting with an Index adaptor as a visibility handler. This
	 * 
	 * @param fieldVisiblityHandler
	 * @param visibilityAttribute
	 * @param visibilityManagement
	 */
	public AdaptorProxyFieldLevelVisibilityHandler(
			final String fieldName,
			final FeatureDataAdapter adapter ) {
		super();
		this.fieldName = fieldName;
		this.adapter = adapter;
	}

	@Override
	public byte[] getVisibility(
			SimpleFeature rowValue,
			ByteArrayId fieldId,
			Object fieldValue ) {

		if (myDeferredHandler == null) {
			myDeferredHandler = adapter.getFieldVisibilityManagement().createVisibilityHandler(
					fieldName,
					adapter.getFieldVisiblityHandler(),
					adapter.getVisibilityAttributeName());
		}
		return myDeferredHandler.getVisibility(
				rowValue,
				fieldId,
				fieldValue);
	}

}
