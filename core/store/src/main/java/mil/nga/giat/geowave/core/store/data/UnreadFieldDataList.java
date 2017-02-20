package mil.nga.giat.geowave.core.store.data;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.store.flatten.FlattenedFieldInfo;
import mil.nga.giat.geowave.core.store.flatten.FlattenedUnreadData;

public class UnreadFieldDataList implements
		FlattenedUnreadData
{
	private final List<FlattenedUnreadData> unreadData;
	private List<FlattenedFieldInfo> cachedRead;

	public UnreadFieldDataList(
			final List<FlattenedUnreadData> unreadData ) {
		this.unreadData = unreadData;
	}

	@Override
	public List<FlattenedFieldInfo> finishRead() {
		if (cachedRead == null) {
			cachedRead = new ArrayList<>();
			for (final FlattenedUnreadData d : unreadData) {
				cachedRead.addAll(d.finishRead());
			}
		}
		return cachedRead;
	}
}
