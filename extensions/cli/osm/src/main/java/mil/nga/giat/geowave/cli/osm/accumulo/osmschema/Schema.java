package mil.nga.giat.geowave.cli.osm.accumulo.osmschema;

import org.apache.accumulo.core.data.ByteSequence;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class Schema
{
	public static final ColumnFamily CF = new ColumnFamily();
	public static final ColumnQualifier CQ = new ColumnQualifier();
	protected static final HashFunction _hf = Hashing.murmur3_128(1);

	public static byte[] getIdHash(
			long id ) {
		return _hf.hashLong(
				id).asBytes();
	}

	public static boolean arraysEqual(
			ByteSequence array,
			byte[] value ) {
		if (value.length != array.length()) {
			return false;
		}
		return startsWith(
				array,
				value);
	}

	public static boolean startsWith(
			ByteSequence array,
			byte[] prefix ) {
		if (prefix.length > array.length()) {
			return false;
		}

		for (int i = 0; i < prefix.length; i++) {
			if (prefix[i] != array.byteAt(i)) {
				return false;
			}
		}
		return true;
	}

}
