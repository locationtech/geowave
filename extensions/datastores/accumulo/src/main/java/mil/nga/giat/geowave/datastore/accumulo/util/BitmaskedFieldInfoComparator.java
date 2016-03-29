package mil.nga.giat.geowave.datastore.accumulo.util;

import java.util.BitSet;
import java.util.Comparator;

import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;

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
		Comparator<FieldInfo<Object>>,
		java.io.Serializable
{
	private static final long serialVersionUID = 1L;

	@Override
	public int compare(
			final FieldInfo<Object> o1,
			final FieldInfo<Object> o2 ) {
		final BitSet lhs = BitSet.valueOf(o1.getDataValue().getId().getBytes());
		final BitSet rhs = BitSet.valueOf(o2.getDataValue().getId().getBytes());
		if (lhs.equals(rhs)) {
			return 0;
		}
		final BitSet xor = (BitSet) lhs.clone();
		xor.xor(rhs);
		final int firstDifferent = xor.length() - 1;
		return rhs.get(firstDifferent) ? -1 : 1;
	}

}
