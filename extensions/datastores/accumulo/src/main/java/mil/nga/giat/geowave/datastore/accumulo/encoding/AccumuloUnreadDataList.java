package mil.nga.giat.geowave.datastore.accumulo.encoding;

import java.util.ArrayList;
import java.util.List;

public class AccumuloUnreadDataList implements
		AccumuloUnreadData
{
	private final List<AccumuloUnreadData> unreadData;
	private List<AccumuloFieldInfo> cachedRead;

	public AccumuloUnreadDataList(
			final List<AccumuloUnreadData> unreadData ) {
		this.unreadData = unreadData;
	}

	@Override
	public List<AccumuloFieldInfo> finishRead() {
		if (cachedRead == null) {
			cachedRead = new ArrayList<>();
			for (final AccumuloUnreadData d : unreadData) {
				cachedRead.addAll(d.finishRead());
			}
		}
		return cachedRead;
	}
}
