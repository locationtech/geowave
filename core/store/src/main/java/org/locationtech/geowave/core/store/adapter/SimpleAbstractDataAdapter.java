package org.locationtech.geowave.core.store.adapter;

import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.data.MultiFieldPersistentDataset;
import org.locationtech.geowave.core.store.data.SingleFieldPersistentDataset;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.data.field.PersistableReader;
import org.locationtech.geowave.core.store.data.field.PersistableWriter;
import org.locationtech.geowave.core.store.index.CommonIndexModel;

abstract public class SimpleAbstractDataAdapter<T extends Persistable> implements
    DataTypeAdapter<T> {
  protected static final String SINGLETON_FIELD_NAME = "FIELD";
  private final FieldReader<Object> reader;
  private final FieldWriter<T, Object> writer;

  public SimpleAbstractDataAdapter() {
    super();
    reader = new PersistableReader(dataClassId());
    writer = new PersistableWriter();
  }

  abstract protected short dataClassId();

  @Override
  public FieldWriter<T, Object> getWriter(final String fieldName) {
    return writer;
  }

  @Override
  public FieldReader<Object> getReader(final String fieldName) {
    return reader;
  }

  @Override
  public T decode(final IndexedAdapterPersistenceEncoding data, final Index index) {
    return (T) data.getAdapterExtendedData().getValue(SINGLETON_FIELD_NAME);
  }

  @Override
  public AdapterPersistenceEncoding encode(final T entry, final CommonIndexModel indexModel) {
    return new AdapterPersistenceEncoding(
        getDataId(entry),
        new MultiFieldPersistentDataset<>(),
        new SingleFieldPersistentDataset<>(SINGLETON_FIELD_NAME, entry));
  }

  @Override
  public int getPositionOfOrderedField(final CommonIndexModel model, final String fieldName) {
    return 0;
  }

  @Override
  public String getFieldNameForPosition(final CommonIndexModel model, final int position) {
    return SINGLETON_FIELD_NAME;
  }
}
