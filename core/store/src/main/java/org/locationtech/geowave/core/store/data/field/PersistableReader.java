package org.locationtech.geowave.core.store.data.field;

import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistableFactory;

public class PersistableReader<F extends Persistable> implements FieldReader<F> {
  private final short classId;

  public PersistableReader(final short classId) {
    super();
    this.classId = classId;
  }

  @Override
  public F readField(final byte[] fieldData) {
    final F newInstance = (F) PersistableFactory.getInstance().newInstance(classId);
    newInstance.fromBinary(fieldData);
    return newInstance;
  }

}
