package mil.nga.giat.geowave.datastore.dynamodb.util;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;

public class DynamoDBUtilsTest
{
	@Test
	public void testSortableBase64EncodeDecode() {
		final String testString = new String(
				"Test converting to and from sortable base64.");
		final byte[] testValue = testString.getBytes();
		final byte[] encoded = DynamoDBUtils.encodeSortableBase64(testValue);
		assertTrue(encoded.length > testValue.length);
		final byte[] decoded = DynamoDBUtils.decodeSortableBase64(encoded);
		final String decodedString = new String(
				decoded);
		assertTrue(testString.equals(decodedString));
	}

	@Test
	public void testSortableBase64Sorting() {
		final List<byte[]> sortedBinary = Lists.newArrayList();
		for (int i = 0; i < Short.MAX_VALUE; i += 100) {
			final byte[] bytes = new byte[2];
			bytes[1] = (byte)(i & 0xff);
			bytes[0] = (byte)((i >> 8) & 0xff);
			sortedBinary.add(bytes);
		}
		for (int i = Short.MIN_VALUE; i < 0; i += 100) {
			final byte[] bytes = new byte[2];
			bytes[1] = (byte)(i & 0xff);
			bytes[0] = (byte)((i >> 8) & 0xff);
			sortedBinary.add(bytes);
		}
		verifySorted(sortedBinary);
		final List<byte[]> encodedBinary = Lists.transform(sortedBinary, (binary) -> DynamoDBUtils.encodeSortableBase64(binary));
		verifySorted(encodedBinary);
	}

	private void verifySorted(
			final List<byte[]> list ) {
		byte[] last = null;
		for (final byte[] binary : list) {
			if (last != null) {
				boolean less = false;
				for (int i = 0; (i < last.length) & (i < binary.length); i++) {
					if ((binary[i] & 0xFF) < (last[i] & 0xFF)) {
						fail();
					}
					else if ((binary[i] & 0xFF) > (last[i] & 0xFF)) {
						less = true;
						break;
					}
				}
				if (!less && (binary.length > last.length)) {
					fail();
				}
			}
			last = binary;
		}
	}
}
