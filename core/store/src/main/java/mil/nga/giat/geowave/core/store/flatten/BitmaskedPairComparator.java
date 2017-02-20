package mil.nga.giat.geowave.core.store.flatten;

import java.util.Comparator;

import org.apache.commons.lang3.tuple.Pair;

/**
 * Comparator to sort FieldInfo's accordingly. Assumes
 * FieldInfo.getDataValue().getId().getBytes() returns the bitmasked
 * representation of a fieldId
 * 
 * @see BitmaskUtils
 * 
 * @since 0.9.1
 */
public class BitmaskedPairComparator implements
		Comparator<Pair<Integer, ?>>,
		java.io.Serializable
{
	private static final long serialVersionUID = 1L;

	@Override
	public int compare(
			final Pair<Integer, ?> o1,
			final Pair<Integer, ?> o2 ) {
		return o1.getLeft().compareTo(
				o2.getLeft());
	}

}
