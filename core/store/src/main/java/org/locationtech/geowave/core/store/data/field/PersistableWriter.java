package org.locationtech.geowave.core.store.data.field;

import org.locationtech.geowave.core.index.persist.Persistable;

public class PersistableWriter<F extends Persistable> implements FieldWriter<Object, F> {

  @Override
  public byte[] writeField(final F fieldValue) {
    if (fieldValue == null) {
      return new byte[0];
    }
    return fieldValue.toBinary();
  }

}
