/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster.resize;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Iterator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.locationtech.geowave.adapter.raster.adapter.ClientMergeableRasterTile;
import org.locationtech.geowave.adapter.raster.adapter.GridCoverageWritable;
import org.locationtech.geowave.adapter.raster.adapter.RasterDataAdapter;
import org.locationtech.geowave.adapter.raster.adapter.merge.nodata.NoDataMergeStrategy;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.metadata.InternalAdapterStoreImpl;
import org.locationtech.geowave.mapreduce.HadoopWritableSerializer;
import org.locationtech.geowave.mapreduce.JobContextAdapterStore;
import org.locationtech.geowave.mapreduce.JobContextIndexStore;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.locationtech.geowave.mapreduce.output.GeoWaveOutputKey;
import org.opengis.coverage.grid.GridCoverage;

public class RasterTileResizeHelper implements Serializable {
  private static final long serialVersionUID = 1L;
  private RasterDataAdapter newAdapter;
  private short oldAdapterId;
  private short newAdapterId;
  private Index index;
  private String[] indexNames;
  private HadoopWritableSerializer<GridCoverage, GridCoverageWritable> serializer;

  public RasterTileResizeHelper(final JobContext context) {
    index = JobContextIndexStore.getIndices(context)[0];
    indexNames = new String[] {index.getName()};
    final DataTypeAdapter[] adapters = JobContextAdapterStore.getDataAdapters(context);
    final Configuration conf = context.getConfiguration();
    final String newTypeName = conf.get(RasterTileResizeJobRunner.NEW_TYPE_NAME_KEY);
    oldAdapterId = (short) conf.getInt(RasterTileResizeJobRunner.OLD_ADAPTER_ID_KEY, -1);
    newAdapterId =
        (short) conf.getInt(
            RasterTileResizeJobRunner.NEW_ADAPTER_ID_KEY,
            InternalAdapterStoreImpl.getLazyInitialAdapterId(newTypeName));
    for (final DataTypeAdapter adapter : adapters) {
      if (adapter.getTypeName().equals(newTypeName)) {
        if (((RasterDataAdapter) adapter).getTransform() == null) {
          // the new adapter doesn't have a merge strategy - resizing
          // will require merging, so default to NoDataMergeStrategy
          newAdapter =
              new RasterDataAdapter(
                  (RasterDataAdapter) adapter,
                  newTypeName,
                  new NoDataMergeStrategy());
        } else {
          newAdapter = (RasterDataAdapter) adapter;
        }
      }
    }
  }

  public RasterTileResizeHelper(
      final short oldAdapterId,
      final short newAdapterId,
      final RasterDataAdapter newAdapter,
      final Index index) {
    this.newAdapter = newAdapter;
    this.oldAdapterId = oldAdapterId;
    this.newAdapterId = newAdapterId;
    this.index = index;
    indexNames = new String[] {index.getName()};
  }

  public GeoWaveOutputKey getGeoWaveOutputKey() {
    return new GeoWaveOutputKey(newAdapter.getTypeName(), indexNames);
  }

  public Iterator<GridCoverage> getCoveragesForIndex(final GridCoverage existingCoverage) {
    return newAdapter.convertToIndex(index, existingCoverage);
  }

  public GridCoverage getMergedCoverage(final GeoWaveInputKey key, final Iterable<Object> values)
      throws IOException, InterruptedException {
    GridCoverage mergedCoverage = null;
    ClientMergeableRasterTile<?> mergedTile = null;
    boolean needsMerge = false;
    final Iterator it = values.iterator();
    while (it.hasNext()) {
      final Object value = it.next();
      if (value instanceof GridCoverage) {
        if (mergedCoverage == null) {
          mergedCoverage = (GridCoverage) value;
        } else {
          if (!needsMerge) {
            mergedTile = newAdapter.getRasterTileFromCoverage(mergedCoverage);
            needsMerge = true;
          }
          final ClientMergeableRasterTile thisTile =
              newAdapter.getRasterTileFromCoverage((GridCoverage) value);
          if (mergedTile != null) {
            mergedTile.merge(thisTile);
          }
        }
      }
    }
    if (needsMerge) {
      final Pair<byte[], byte[]> pair = key.getPartitionAndSortKey(index);
      mergedCoverage =
          newAdapter.getCoverageFromRasterTile(
              mergedTile,
              pair == null ? null : pair.getLeft(),
              pair == null ? null : pair.getRight(),
              index);
    }
    return mergedCoverage;
  }

  private void readObject(final ObjectInputStream aInputStream)
      throws ClassNotFoundException, IOException {
    final byte[] adapterBytes = new byte[aInputStream.readUnsignedShort()];
    aInputStream.readFully(adapterBytes);
    final byte[] indexBytes = new byte[aInputStream.readUnsignedShort()];
    aInputStream.readFully(indexBytes);
    newAdapter = (RasterDataAdapter) PersistenceUtils.fromBinary(adapterBytes);
    index = (Index) PersistenceUtils.fromBinary(indexBytes);
    oldAdapterId = aInputStream.readShort();
    newAdapterId = aInputStream.readShort();
    indexNames = new String[] {index.getName()};
  }

  private void writeObject(final ObjectOutputStream aOutputStream) throws IOException {
    final byte[] adapterBytes = PersistenceUtils.toBinary(newAdapter);
    final byte[] indexBytes = PersistenceUtils.toBinary(index);
    aOutputStream.writeShort(adapterBytes.length);
    aOutputStream.write(adapterBytes);
    aOutputStream.writeShort(indexBytes.length);
    aOutputStream.write(indexBytes);
    aOutputStream.writeShort(oldAdapterId);
    aOutputStream.writeShort(newAdapterId);
    aOutputStream.flush();
  }

  public HadoopWritableSerializer<GridCoverage, GridCoverageWritable> getSerializer() {
    if (serializer == null) {
      serializer = newAdapter.createWritableSerializer();
    }
    return serializer;
  }

  public short getNewAdapterId() {
    return newAdapterId;
  }

  public byte[] getNewDataId(final GridCoverage coverage) {
    return newAdapter.getDataId(coverage);
  }

  public String getIndexName() {
    return index.getName();
  }

  public boolean isOriginalCoverage(final short adapterId) {
    return oldAdapterId == adapterId;
  }
}
