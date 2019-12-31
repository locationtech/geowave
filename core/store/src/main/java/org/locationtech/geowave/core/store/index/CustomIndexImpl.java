package org.locationtech.geowave.core.store.index;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;

public class CustomIndexImpl<E, C extends Persistable> extends NullIndex implements
    CustomIndex<E, C> {
  private CustomIndexStrategy<E, C> indexStrategy;

  public CustomIndexImpl() {
    super();
  }

  public CustomIndexImpl(final CustomIndexStrategy<E, C> indexStrategy, final String id) {
    super(id);
    this.indexStrategy = indexStrategy;
  }

  @Override
  public InsertionIds getInsertionIds(final E entry) {
    return indexStrategy.getInsertionIds(entry);
  }

  @Override
  public QueryRanges getQueryRanges(final C constraints) {
    return indexStrategy.getQueryRanges(constraints);
  }

  @Override
  public int hashCode() {
    return getName().hashCode();
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final IndexImpl other = (IndexImpl) obj;
    return getName().equals(other.getName());
  }

  @Override
  public byte[] toBinary() {
    final byte[] baseBinary = super.toBinary();
    final byte[] additionalBinary = PersistenceUtils.toBinary(indexStrategy);
    final ByteBuffer buf =
        ByteBuffer.allocate(
            VarintUtils.unsignedIntByteLength(baseBinary.length)
                + baseBinary.length
                + additionalBinary.length);
    VarintUtils.writeUnsignedInt(baseBinary.length, buf);
    buf.put(baseBinary);
    buf.put(additionalBinary);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final byte[] baseBinary = ByteArrayUtils.safeRead(buf, VarintUtils.readUnsignedInt(buf));
    super.fromBinary(baseBinary);
    final byte[] additionalBinary = ByteArrayUtils.safeRead(buf, buf.remaining());
    indexStrategy = (CustomIndexStrategy<E, C>) PersistenceUtils.fromBinary(additionalBinary);
  }

}
