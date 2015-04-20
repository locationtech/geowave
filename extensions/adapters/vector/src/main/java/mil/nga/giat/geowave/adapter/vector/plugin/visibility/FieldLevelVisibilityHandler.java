package mil.nga.giat.geowave.adapter.vector.plugin.visibility;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.visibility.VisibilityManagement;

import org.opengis.feature.simple.SimpleFeature;

/**
 * Define visibility for a specific attribute using the
 * {@link VisibilityManagement}. The visibility is determined by meta-data in a
 * separate feature attribute.
 * 
 * @see JsonDefinitionColumnVisibilityManagement
 * 
 * 
 * 
 * @param <T>
 * @param <CommonIndexValue>
 */
public class FieldLevelVisibilityHandler<T, CommonIndexValue> implements
		FieldVisibilityHandler<T, CommonIndexValue>
{

	private final String visibilityAttribute;
	private final String fieldName;
	private final VisibilityManagement<T> visibilityManagement;
	private FieldVisibilityHandler<T, Object> defaultFieldVisiblityHandler;

	/**
	 * Used when acting with an Index adaptor as a visibility handler. This
	 * 
	 * @param fieldName
	 *            - the name of the field for which to set determine the
	 *            visibility.
	 * @param fieldVisiblityHandler
	 *            default visibility handler if a specific visibility cannot be
	 *            determined from the contents of the attribute used to
	 *            determine visibility (name providied by parameter
	 *            'visibilityAttribute')
	 * @param visibilityAttribute
	 *            the attribute name that contains data to discern visibility
	 *            for other field/attributes.
	 * @param visibilityManagement
	 */
	public FieldLevelVisibilityHandler(
			final String fieldName,
			final FieldVisibilityHandler<T, Object> fieldVisiblityHandler,
			final String visibilityAttribute,
			final VisibilityManagement<T> visibilityManagement ) {
		super();
		this.fieldName = fieldName;
		this.visibilityAttribute = visibilityAttribute;
		this.visibilityManagement = visibilityManagement;
		this.defaultFieldVisiblityHandler = fieldVisiblityHandler;
	}

	@Override
	public byte[] getVisibility(
			T rowValue,
			ByteArrayId fieldId,
			CommonIndexValue fieldValue ) {

		SimpleFeature feature = (SimpleFeature) rowValue;
		Object visibilityAttributeValue = feature.getAttribute(this.visibilityAttribute);
		byte[] result = visibilityAttributeValue != null ? visibilityManagement.translateVisibility(
				visibilityAttributeValue,
				fieldName) : null;
		return result != null ? result : (defaultFieldVisiblityHandler == null ? new byte[] {} : defaultFieldVisiblityHandler.getVisibility(
				rowValue,
				fieldId,
				fieldValue));
	}

}
