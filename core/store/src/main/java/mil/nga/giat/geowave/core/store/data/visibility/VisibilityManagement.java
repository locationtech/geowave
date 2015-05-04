package mil.nga.giat.geowave.core.store.data.visibility;

import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

/**
 * Provides a single consolidated tool to
 * 
 * (1) determine the visibility for a specific field. (2) provide
 * {@link VisibilityTransformationIterator} to transform visibilities. A
 * subclass of the {@link ColumnVisibilityManagement} provides transforming
 * functions for visibilities defined in terms of Accumulo Column Visibility,
 * without any alternate interpretive adaptors. Thus, visibility is defined in
 * terms of an expression (e.g. a&b|c). However, Accumulo users may wish to
 * define an alternate interpreter for visibility along with an alternate
 * format. In this case, the transformation would need to be redefined by a
 * different implementation.
 * 
 * 
 * 
 * 
 */
public interface VisibilityManagement<T>
{
	/**
	 * 
	 * @param visibilityObject
	 *            an object that defines visibility for each field
	 * @param fieldName
	 *            the field to which visibility is being requested
	 * @return null if the default should be used, otherwise return the
	 *         visibility for the provide field given the instructions found in
	 *         the visibilityObject
	 */
	public byte[] translateVisibility(
			Object visibilityObject,
			String fieldName );

/** 
	 * Create a wrapper visibility writer.
	 * In most cases, the return value should be {@link FieldLevelVisibilityWriter} 
	 * using the {@link FieldVisibilityHandler} created by {@link VisibilityManagement#createVisibilityHandler(String, FieldVisibilityHandler, String)}
	 * 
	 * The motivation to provide an alternate implementation is to support a strategy where
	 * each field carries its own marking.  In such a case, the method {@link VisibilityManagement#translateVisibility(Object, String)
	 * would not be used since 
	 * 
	 * 
	 * @param fieldName
	 * @param writer
	 * @param defaultFieldVisiblityHandler
	 * @param visibilityAttribute
	 * @return
	 */
	public FieldLevelVisibilityWriter<T, Object> createVisibilityWriter(
			final String fieldName,
			final FieldWriter<T, Object> writer,
			FieldVisibilityHandler<T, Object> defaultFieldVisiblityHandler,
			final String visibilityAttribute );

	/**
	 * Create a visibility handler
	 * 
	 * @param fieldName
	 *            -- the name of the field for object of type T requiring
	 *            visibility treatment
	 * @param defaultHandler
	 *            -- default handler if the visibilityAttributeName is not
	 *            provided or the field does not exist in a provided object of
	 *            type T.
	 * @param visibilityAttributeName
	 *            -- optional name of a field that determines visibility of each
	 *            field in an object of type T
	 * @return
	 */
	public FieldVisibilityHandler<T, Object> createVisibilityHandler(
			String fieldName,
			FieldVisibilityHandler<T, Object> defaultHandler,
			String visibilityAttributeName );
}
