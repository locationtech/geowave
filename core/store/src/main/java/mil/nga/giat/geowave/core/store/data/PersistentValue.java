package mil.nga.giat.geowave.core.store.data;

import mil.nga.giat.geowave.core.index.ByteArrayId;

/**
 * This represents a single value in the GeoWave data store as the value plus
 * the field ID pair
 * 
 * @param <T>
 *            The binding class for this value
 */
public class PersistentValue<T>
{
	private final ByteArrayId id;
	private final T value;

	public PersistentValue(
			final ByteArrayId id,
			final T value ) {
		this.id = id;
		this.value = value;
	}

	/**
	 * Return the field ID
	 * 
	 * @return the field ID
	 */
	public ByteArrayId getId() {
		return id;
	}

	/**
	 * Return the value
	 * 
	 * @return the value
	 */
	public T getValue() {
		return value;
	}
}
