package mil.nga.giat.geowave.core.store.data;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.core.index.ByteArrayId;

/**
 * This is a basic mapping of field ID to native field type. "Native" in this
 * sense can be to either the data adapter or the common index, depending on
 * whether it is in the common index or is an extended field.
 * 
 * @param <T>
 *            The most specific generalization for the type for all of the
 *            values in this dataset.
 */
public class PersistentDataset<T>
{
	private final Map<ByteArrayId, T> fieldIdToValueMap;

	public PersistentDataset() {
		// to maintain order use a linked hashmap
		fieldIdToValueMap = new LinkedHashMap<ByteArrayId, T>();
	}

	public PersistentDataset(
			final Map<ByteArrayId, T> fieldIdToValueMap ) {
		this.fieldIdToValueMap = fieldIdToValueMap;
	}

	/**
	 * Add the field ID/value pair to this data set. Do not overwrite.
	 * 
	 * @param value
	 *            the field ID/value pair to add
	 */
	public void addValue(
			final PersistentValue<T> value ) {
		if (fieldIdToValueMap.containsKey(value.getId())) return;
		fieldIdToValueMap.put(
				value.getId(),
				value.getValue());
	}

	/**
	 * Add or update the field ID/value pair to this data set
	 * 
	 * @param value
	 *            the field ID/value pair to add
	 */
	public void addOrUpdateValue(
			final PersistentValue<T> value ) {
		fieldIdToValueMap.put(
				value.getId(),
				value.getValue());
	}

	/**
	 * Given a field ID, get the associated value
	 * 
	 * @param fieldId
	 *            the field ID
	 * @return the stored field value, null if this does not contain a value for
	 *         the ID
	 */
	public T getValue(
			final ByteArrayId fieldId ) {
		return fieldIdToValueMap.get(fieldId);
	}

	/**
	 * Get all of the values from this persistent data set
	 * 
	 * @return all of the value
	 */
	public List<PersistentValue<T>> getValues() {
		final List<PersistentValue<T>> values = new ArrayList<PersistentValue<T>>(
				fieldIdToValueMap.size());
		for (final Entry<ByteArrayId, T> entry : fieldIdToValueMap.entrySet()) {
			values.add(new PersistentValue<T>(
					entry.getKey(),
					entry.getValue()));
		}
		return values;
	}
}
