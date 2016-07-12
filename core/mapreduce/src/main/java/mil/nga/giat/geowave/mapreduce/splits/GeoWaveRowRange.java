package mil.nga.giat.geowave.mapreduce.splits;

import org.apache.hadoop.io.Writable;

public interface GeoWaveRowRange extends
		Writable
{
	byte[] getStartKey();

	byte[] getEndKey();

	boolean isStartKeyInclusive();

	boolean isEndKeyInclusive();

	boolean isInfiniteStartKey();

	boolean isInfiniteStopKey();
}
