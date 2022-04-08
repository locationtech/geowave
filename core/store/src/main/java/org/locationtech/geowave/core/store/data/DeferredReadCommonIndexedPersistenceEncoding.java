/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.data;

import java.util.List;
import org.locationtech.geowave.core.store.adapter.AbstractAdapterPersistenceEncoding;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.flatten.FlattenedFieldInfo;
import org.locationtech.geowave.core.store.flatten.FlattenedUnreadData;
import org.locationtech.geowave.core.store.index.CommonIndexModel;

/**
 * Consults adapter to lookup field readers based on bitmasked fieldIds when converting unknown data
 * to adapter extended values
 *
 * @since 0.9.1
 */
public class DeferredReadCommonIndexedPersistenceEncoding extends
    AbstractAdapterPersistenceEncoding {

  private final FlattenedUnreadData unreadData;

  public DeferredReadCommonIndexedPersistenceEncoding(
      final short adapterId,
      final byte[] dataId,
      final byte[] partitionKey,
      final byte[] sortKey,
      final int duplicateCount,
      final PersistentDataset<Object> commonData,
      final FlattenedUnreadData unreadData) {
    super(
        adapterId,
        dataId,
        partitionKey,
        sortKey,
        duplicateCount,
        commonData,
        new MultiFieldPersistentDataset<byte[]>(),
        new MultiFieldPersistentDataset<>());
    this.unreadData = unreadData;
  }

  @Override
  public void convertUnknownValues(
      final InternalDataAdapter<?> adapter,
      final CommonIndexModel model) {
    if (unreadData != null) {
      final List<FlattenedFieldInfo> fields = unreadData.finishRead();
      for (final FlattenedFieldInfo field : fields) {
        String fieldName = adapter.getFieldNameForPosition(model, field.getFieldPosition());
        if (fieldName == null) {
          fieldName = adapter.getFieldNameForPosition(model, field.getFieldPosition());
        }
        final FieldReader<Object> reader = adapter.getReader(fieldName);
        final Object value = reader.readField(field.getValue());
        adapterExtendedData.addValue(fieldName, value);
      }
    }
  }
}
