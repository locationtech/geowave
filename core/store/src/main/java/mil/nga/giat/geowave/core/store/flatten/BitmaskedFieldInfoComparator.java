package mil.nga.giat.geowave.core.store.flatten;

import java.util.Comparator;

import org.apache.commons.lang3.tuple.Pair;

import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;

/**
 * Comparator to sort FieldInfo's accordingly. Assumes
 * FieldInfo.getDataValue().getId().getBytes() returns the bitmasked
 * representation of a fieldId
 * 
 * @see BitmaskUtils
 * 
 * @since 0.9.1
 */
public class BitmaskedFieldInfoComparator implements
		Comparator<Pair<Integer, FieldInfo<?>>>,
		java.io.Serializable
{
	private static final long serialVersionUID = 1L;

	@Override
	public int compare(
			final Pair<Integer, FieldInfo<?>> o1,
			final Pair<Integer, FieldInfo<?>> o2 ) {
		return o1.getLeft().compareTo(
				o2.getLeft());
	}

}
