/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster.adapter;

import org.locationtech.geowave.adapter.raster.FitToIndexGridCoverage;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.adapter.AdapterPersistenceEncoding;
import org.locationtech.geowave.core.store.adapter.FitToIndexPersistenceEncoding;
import org.locationtech.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapterImpl;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.VisibilityHandler;
import org.locationtech.geowave.core.store.data.MultiFieldPersistentDataset;
import org.locationtech.geowave.core.store.data.PersistentDataset;
import org.locationtech.geowave.core.store.data.SingleFieldPersistentDataset;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.opengis.coverage.grid.GridCoverage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InternalRasterDataAdapter extends InternalDataAdapterImpl<GridCoverage> {
  private static final Logger LOGGER = LoggerFactory.getLogger(InternalRasterDataAdapter.class);

  public InternalRasterDataAdapter() {}

  public InternalRasterDataAdapter(final RasterDataAdapter adapter, final short adapterId) {
    super(adapter, adapterId);
  }

  public InternalRasterDataAdapter(
      final RasterDataAdapter adapter,
      final short adapterId,
      final VisibilityHandler visibilityHandler) {
    super(adapter, adapterId, visibilityHandler);
  }

  @Override
  public GridCoverage decode(
      final IndexedAdapterPersistenceEncoding data,
      final AdapterToIndexMapping indexMapping,
      final Index index) {
    final Object rasterTile =
        data.getAdapterExtendedData().getValue(RasterDataAdapter.DATA_FIELD_ID);
    if ((rasterTile == null) || !(rasterTile instanceof RasterTile)) {
      return null;
    }
    return ((RasterDataAdapter) adapter).getCoverageFromRasterTile(
        (RasterTile) rasterTile,
        data.getInsertionPartitionKey(),
        data.getInsertionSortKey(),
        index);
  }

  @Override
  public AdapterPersistenceEncoding encode(
      final GridCoverage entry,
      final AdapterToIndexMapping indexMapping,
      final Index index) {
    final PersistentDataset<Object> adapterExtendedData = new SingleFieldPersistentDataset<>();
    adapterExtendedData.addValue(
        RasterDataAdapter.DATA_FIELD_ID,
        ((RasterDataAdapter) adapter).getRasterTileFromCoverage(entry));
    final AdapterPersistenceEncoding encoding;
    if (entry instanceof FitToIndexGridCoverage) {
      encoding =
          new FitToIndexPersistenceEncoding(
              getAdapterId(),
              new byte[0],
              new MultiFieldPersistentDataset<>(),
              adapterExtendedData,
              ((FitToIndexGridCoverage) entry).getPartitionKey(),
              ((FitToIndexGridCoverage) entry).getSortKey());
    } else {
      // this shouldn't happen
      LOGGER.warn("Grid coverage is not fit to the index");
      encoding =
          new AdapterPersistenceEncoding(
              getAdapterId(),
              new byte[0],
              new MultiFieldPersistentDataset<>(),
              adapterExtendedData);
    }
    return encoding;
  }

  @Override
  public boolean isCommonIndexField(
      final AdapterToIndexMapping indexMapping,
      final String fieldName) {
    return false;
  }

  @Override
  public int getPositionOfOrderedField(final CommonIndexModel model, final String fieldName) {
    int i = 0;
    for (final NumericDimensionField<?> dimensionField : model.getDimensions()) {
      if (fieldName.equals(dimensionField.getFieldName())) {
        return i;
      }
      i++;
    }
    if (fieldName.equals(RasterDataAdapter.DATA_FIELD_ID)) {
      return i;
    }
    return -1;
  }

  @Override
  public String getFieldNameForPosition(final CommonIndexModel model, final int position) {
    if (position < model.getDimensions().length) {
      int i = 0;
      for (final NumericDimensionField<?> dimensionField : model.getDimensions()) {
        if (i == position) {
          return dimensionField.getFieldName();
        }
        i++;
      }
    } else {
      final int numDimensions = model.getDimensions().length;
      if (position == numDimensions) {
        return RasterDataAdapter.DATA_FIELD_ID;
      }
    }
    return null;
  }
}
