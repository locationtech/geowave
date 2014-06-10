package mil.nga.giat.geowave.index.sfc.zorder;

import java.util.BitSet;

/**
 * Convenience methods used to decode/encode Z-Order space filling curve values
 * (using a simple bit-interleaving approach).
 * 
 */
public class ZOrderUtils
{
	public static double[] decode(
			byte[] bytes,
			int bitsPerDimension,
			int numDimensions ) {
		byte[] littleEndianBytes = swapEndianFormat(bytes);
		BitSet bitSet = BitSet.valueOf(littleEndianBytes);
		double[] normalizedValues = new double[numDimensions];
		for (int d = 0; d < numDimensions; d++) {
			BitSet dimensionSet = new BitSet();
			int j = 0;
			for (int i = d; i < bitsPerDimension * numDimensions; i += numDimensions) {
				dimensionSet.set(
						j++,
						bitSet.get(i));
			}

			normalizedValues[d] = decode(
					dimensionSet,
					0,
					1);
		}

		return normalizedValues;
	}

	private static double decode(
			BitSet bs,
			double floor,
			double ceiling ) {
		double mid = 0;
		for (int i = 0; i < bs.length(); i++) {
			mid = (floor + ceiling) / 2;
			if (bs.get(i))
				floor = mid;
			else
				ceiling = mid;
		}
		return mid;
	}

	public static byte[] encode(
			double[] normalizedValues,
			int bitsPerDimension,
			int numDimensions ) {
		BitSet[] bitSets = new BitSet[numDimensions];

		for (int d = 0; d < numDimensions; d++) {
			bitSets[d] = getBits(
					normalizedValues[d],
					0,
					1,
					bitsPerDimension);
		}
		// BitVector bitVector =
		// BitVectorFactories.OPTIMAL.apply(bitsPerDimension * numDimensions);
		BitSet combinedBitSet = new BitSet(
				bitsPerDimension * numDimensions);
		int j = 0;
		for (int i = 0; i < bitsPerDimension; i++) {
			for (int d = 0; d < numDimensions; d++) {
				combinedBitSet.set(
						j++,
						bitSets[d].get(i));
			}
		}
		byte[] littleEndianBytes = combinedBitSet.toByteArray();
		return swapEndianFormat(littleEndianBytes);
	}

	public static byte[] swapEndianFormat(
			byte[] b ) {
		byte[] endianSwappedBytes = new byte[b.length];
		for (int i = 0; i < b.length; i++) {
			endianSwappedBytes[i] = swapEndianFormat(b[i]);
		}
		return endianSwappedBytes;
	}

	private static byte swapEndianFormat(
			byte b ) {
		int converted = 0x00;
		converted ^= (b & 0b1000_0000) >> 7;
		converted ^= (b & 0b0100_0000) >> 5;
		converted ^= (b & 0b0010_0000) >> 3;
		converted ^= (b & 0b0001_0000) >> 1;
		converted ^= (b & 0b0000_1000) << 1;
		converted ^= (b & 0b0000_0100) << 3;
		converted ^= (b & 0b0000_0010) << 5;
		converted ^= (b & 0b0000_0001) << 7;
		return (byte) (converted & 0xFF);
	}

	private static BitSet getBits(
			double value,
			double floor,
			double ceiling,
			int bitsPerDimension ) {
		BitSet buffer = new BitSet(
				bitsPerDimension);
		for (int i = 0; i < bitsPerDimension; i++) {
			double mid = (floor + ceiling) / 2;
			if (value >= mid) {
				buffer.set(i);
				floor = mid;
			}
			else {
				ceiling = mid;
			}
		}
		return buffer;
	}

}