package mil.nga.giat.geowave.datastore.accumulo.util;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;

import mil.nga.giat.geowave.core.store.entities.GeoWaveRowImpl;

public class IteratorUtils
{

	public static class SkeletonKey extends
			Key
	{
		public SkeletonKey(
				final Key other ) {
			super(
					other);
		}

		@Override
		public boolean equals(
				Key other,
				final PartialKey part ) {
			final GeoWaveRowImpl myRowId = new GeoWaveRowImpl(
					getRowData().getBackingArray());

			GeoWaveRowImpl otherRowId = new GeoWaveRowImpl(
					other.getRowData().getBackingArray());

			otherRowId = new GeoWaveRowImpl(
					myRowId.getDataId(),
					otherRowId.getAdapterId(),
					otherRowId.getIndex(),
					otherRowId.getNumberOfDuplicates());

			final byte[] cf = other.getColumnFamilyData().toArray();
			final byte[] cq = other.getColumnQualifierData().toArray();
			final byte[] cv = other.getColumnVisibilityData().toArray();
			final long timestamp = other.getTimestamp();

			other = new SkeletonKey(
					new Key(
							otherRowId.getRowId(),
							0,
							otherRowId.getRowId().length,
							cf,
							0,
							cf.length,
							cq,
							0,
							cq.length,
							cv,
							0,
							cv.length,
							timestamp));

			return super.equals(
					other,
					part);
		}
	}

	public static Key replaceRow(
			final Key originalKey,
			final byte[] newRow ) {
		final byte[] row = newRow;
		final byte[] cf = originalKey.getColumnFamilyData().toArray();
		final byte[] cq = originalKey.getColumnQualifierData().toArray();
		final byte[] cv = originalKey.getColumnVisibilityData().toArray();
		final long timestamp = originalKey.getTimestamp();
		final Key newKey = new SkeletonKey(
				new Key(
						row,
						0,
						row.length,
						cf,
						0,
						cf.length,
						cq,
						0,
						cq.length,
						cv,
						0,
						cv.length,
						timestamp));
		newKey.setDeleted(originalKey.isDeleted());
		return newKey;
	}

}
