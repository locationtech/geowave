package mil.nga.giat.geowave.datastore.accumulo.encoding;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class AccumuloUnreadDataSingleRow implements
		AccumuloUnreadData
{
	private final ByteBuffer partiallyConsumedBuffer;
	private final int currentIndexInFieldPositions;
	private final List<Integer> fieldPositions;
	private List<AccumuloFieldInfo> cachedRead = null;

	public AccumuloUnreadDataSingleRow(
			final ByteBuffer partiallyConsumedBuffer,
			final int currentIndexInFieldPositions,
			final List<Integer> fieldPositions ) {
		this.partiallyConsumedBuffer = partiallyConsumedBuffer;
		this.currentIndexInFieldPositions = currentIndexInFieldPositions;
		this.fieldPositions = fieldPositions;
	}

	public List<AccumuloFieldInfo> finishRead() {
		if (cachedRead == null) {
			cachedRead = new ArrayList<>();
			for (int i = currentIndexInFieldPositions; i < fieldPositions.size(); i++) {
				final int fieldLength = partiallyConsumedBuffer.getInt();
				final byte[] fieldValueBytes = new byte[fieldLength];
				partiallyConsumedBuffer.get(fieldValueBytes);
				final Integer fieldPosition = fieldPositions.get(i);
				cachedRead.add(new AccumuloFieldInfo(
						fieldPosition,
						fieldValueBytes));
			}
		}
		return cachedRead;
	}
}
