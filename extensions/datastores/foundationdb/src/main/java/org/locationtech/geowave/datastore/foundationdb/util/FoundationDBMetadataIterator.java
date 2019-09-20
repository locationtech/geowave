package org.locationtech.geowave.datastore.foundationdb.util;

import com.apple.foundationdb.KeyValue;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;

public class FoundationDBMetadataIterator extends AbstractFoundationDBIterator<GeoWaveMetadata> {

  private final boolean containsTimestamp;
  private final boolean visibilityEnabled;

  public FoundationDBMetadataIterator(
      Iterator<KeyValue> it,
      final boolean containsTimestamp,
      final boolean visibilityEnabled) {
    super(it);
    this.containsTimestamp = containsTimestamp;
    this.visibilityEnabled = visibilityEnabled;
  }

  @Override
  protected GeoWaveMetadata readRow(final KeyValue keyValue) {
    final byte[] key = keyValue.getKey();
    final byte[] value = keyValue.getValue();
    final ByteBuffer buf = ByteBuffer.wrap(key);
    final byte[] primaryId = new byte[Byte.toUnsignedInt(key[key.length - 1])];
    final byte[] visibility;

    if (visibilityEnabled) {
      visibility = new byte[Byte.toUnsignedInt(key[key.length - 2])];
    } else {
      visibility = new byte[0];
    }
    int secondaryIdLength = key.length - primaryId.length - visibility.length - 1;
    if (containsTimestamp) {
      secondaryIdLength -= 8;
    }
    if (visibilityEnabled) {
      secondaryIdLength--;
    }
    final byte[] secondaryId = new byte[secondaryIdLength];
    buf.get(primaryId);
    buf.get(secondaryId);
    if (containsTimestamp) {
      // just skip 8 bytes - we don't care to parse out the timestamp but
      // its there for key uniqueness and to maintain expected sort order
      buf.position(buf.position() + 8);
    }
    if (visibilityEnabled) {
      buf.get(visibility);
    }

    return new FoundationDBGeoWaveMetadata(primaryId, secondaryId, visibility, value, key);
  }
}
