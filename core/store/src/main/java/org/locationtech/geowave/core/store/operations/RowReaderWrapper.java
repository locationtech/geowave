package org.locationtech.geowave.core.store.operations;

import org.locationtech.geowave.core.store.CloseableIterator;

public class RowReaderWrapper<T> implements RowReader<T> {
  private final CloseableIterator<T> iterator;

  public RowReaderWrapper(final CloseableIterator<T> iterator) {
    this.iterator = iterator;
  }

  @Override
  public void close() {
    iterator.close();
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public T next() {
    return iterator.next();
  }
}
