/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.hbase.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Scan;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.datastore.hbase.util.HBaseUtils;
import org.locationtech.geowave.mapreduce.URLClassloaderUtils;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class MergingServerOp implements HBaseServerOp {
  public static Object MUTEX = new Object();
  protected Set<GeoWaveColumnId> columnFamilyIds = new HashSet<>();
  // protected Set<ByteArrayId> columnFamilyIds = new HashSet<>();
  private static final String OLD_MAX_VERSIONS_KEY = "MAX_VERSIONS";
  private static final String ROW_DELETES_KEY = "ROW_DELETES";

  protected Mergeable getMergeable(final Cell cell, final byte[] bytes) {
    return (Mergeable) URLClassloaderUtils.fromBinary(bytes);
  }

  protected byte[] getBinary(final Mergeable mergeable) {
    return URLClassloaderUtils.toBinary(mergeable);
  }

  @Override
  public boolean nextRow(final RowScanner rowScanner) throws IOException {
    synchronized (MUTEX) {
      do {
        // a reference variable to all the current cells
        final List<Cell> rowCells = rowScanner.currentCellsInRow();
        final RowDeletes rowDeletes = trackDeletes(rowScanner, rowCells);

        if (rowCells.size() > 1) {
          Integer maxVersions = null;
          if (rowScanner.getScan() != null) {
            final Object oldMaxObj = rowScanner.getHints().get(OLD_MAX_VERSIONS_KEY);
            if ((oldMaxObj == null) || !(oldMaxObj instanceof Integer)) {
              final byte[] oldMaxVersions = rowScanner.getScan().getAttribute(OLD_MAX_VERSIONS_KEY);
              if (oldMaxVersions != null) {
                maxVersions = ByteBuffer.wrap(oldMaxVersions).getInt();
                // cache it in a "hints" map to avoid multiple
                // byte buffer allocations
                rowScanner.getHints().put(OLD_MAX_VERSIONS_KEY, maxVersions);
              }
            } else {
              maxVersions = (Integer) oldMaxObj;
            }
          }
          final Iterator<Cell> iter = rowCells.iterator();
          final Map<PartialCellEquality, List<Cell>> merges = new HashMap<>();
          final Map<PartialCellEquality, List<Cell>> nonMerges = new HashMap<>();
          final List<Cell> unmerged = new ArrayList<>();
          // iterate once to capture individual tags/visibilities
          boolean rebuildList = false;
          while (iter.hasNext()) {
            final Cell cell = iter.next();
            // a delete marker has to survive a flush or compaction unmerged, otherwise it no
            // longer masks the older cells it deleted in other store files. While a scanner older
            // than the marker is open, HBase also hands over the Puts the marker masks; merging
            // one into a newer Put would bring its value back.
            if (CellUtil.isDelete(cell) || ((rowDeletes != null) && rowDeletes.masks(cell))) {
              unmerged.add(cell);
              continue;
            }
            // TODO consider avoiding extra byte array allocations
            final byte[] familyBytes = CellUtil.cloneFamily(cell);
            GeoWaveColumnId familyId = null;
            if (columnFamilyIds.iterator().next() instanceof ShortColumnId) {
              familyId =
                  new ShortColumnId(
                      ByteArrayUtils.shortFromString(StringUtils.stringFromBinary(familyBytes)));
            } else if (columnFamilyIds.iterator().next() instanceof ByteArrayColumnId) {
              familyId = new ByteArrayColumnId(new ByteArray(familyBytes));
            }

            if (columnFamilyIds.contains(familyId)) {
              final PartialCellEquality key = new PartialCellEquality(cell, includeTags());
              List<Cell> cells = merges.get(key);
              if (cells == null) {
                cells = new ArrayList<>();
                merges.put(key, cells);
              } else {
                // this implies there is more than one cell with
                // the
                // same vis, so merging will need to take place
                rebuildList = true;
              }
              cells.add(cell);
            } else {
              // always include tags for non-merge cells so that
              // versioning works as expected
              final PartialCellEquality key = new PartialCellEquality(cell, true);
              // get max versions and trim these cells to max
              // versions
              // per column family and qualifier, and tags
              List<Cell> cells = nonMerges.get(key);
              if (cells == null) {
                cells = new ArrayList<>();
                nonMerges.put(key, cells);
              } else if ((maxVersions != null) && (cells.size() >= maxVersions)) {
                rebuildList = true;
              }
              cells.add(cell);
            }
          }
          if (rebuildList) {
            rowCells.clear();
            for (final List<Cell> cells : merges.values()) {
              if (cells.size() > 1) {
                rowCells.add(mergeList(cells));
              } else if (cells.size() == 1) {
                rowCells.add(cells.get(0));
              }
            }
            for (final List<Cell> cells : nonMerges.values()) {
              if ((maxVersions != null) && (cells.size() > maxVersions)) {
                rowCells.addAll(cells.subList(0, maxVersions));
              } else {
                rowCells.addAll(cells);
              }
            }
            rowCells.addAll(unmerged);
            // these have to stay in order and they can get out of
            // order when adding cells from 2 maps
            rowCells.sort(HBaseUtils.getCellComparator());
          }
        }
      } while (!rowScanner.nextCellsInRow().isEmpty());
      return true;
    }
  }

  /**
   * Adds the delete markers among the current cells to those of the same row in earlier batches. A
   * flush or compaction hands over at most hbase.hstore.compaction.kv.max cells of a row at a time,
   * and a family marker sorts first in its family, so it can come in an earlier batch than the Puts
   * it masks.
   */
  private static RowDeletes trackDeletes(final RowScanner rowScanner, final List<Cell> rowCells) {
    if (rowCells.isEmpty()) {
      return null;
    }
    final Map<String, Object> hints = rowScanner.getHints();
    RowDeletes rowDeletes = (RowDeletes) hints.get(ROW_DELETES_KEY);
    if ((rowDeletes != null) && !CellUtil.matchingRows(rowCells.get(0), rowDeletes.row)) {
      hints.remove(ROW_DELETES_KEY);
      rowDeletes = null;
    }
    for (final Cell cell : rowCells) {
      if (CellUtil.isDelete(cell)) {
        if (rowDeletes == null) {
          rowDeletes = new RowDeletes(CellUtil.cloneRow(cell));
          hints.put(ROW_DELETES_KEY, rowDeletes);
        }
        rowDeletes.add(cell);
      }
    }
    return rowDeletes;
  }

  protected boolean includeTags() {
    return true;
  }

  protected Cell mergeList(final List<Cell> cells) {
    synchronized (MUTEX) {
      Mergeable currentMergeable = null;
      final Cell firstCell = cells.get(0);
      for (final Cell cell : cells) {
        final Mergeable mergeable =
            getMergeable(
                cell,
                // TODO consider avoiding extra byte array
                // allocations (which would require
                // persistence utils to be able to use
                // bytebuffer instead of byte[])
                CellUtil.cloneValue(cell));
        if (mergeable != null) {
          if (currentMergeable == null) {
            currentMergeable = mergeable;
          } else {
            currentMergeable.merge(mergeable);
          }
        }
      }
      final byte[] valueBinary = getBinary(currentMergeable);
      // this is basically a lengthy verbose form of cloning
      // in-place (without allocating new byte arrays) and
      // simply replacing the value with the new mergeable
      // value
      return new KeyValue(
          firstCell.getRowArray(),
          firstCell.getRowOffset(),
          firstCell.getRowLength(),
          firstCell.getFamilyArray(),
          firstCell.getFamilyOffset(),
          firstCell.getFamilyLength(),
          firstCell.getQualifierArray(),
          firstCell.getQualifierOffset(),
          firstCell.getQualifierLength(),
          firstCell.getTimestamp(),
          Type.codeToType(firstCell.getTypeByte()),
          valueBinary,
          0,
          valueBinary.length,
          firstCell.getTagsArray(),
          firstCell.getTagsOffset(),
          firstCell.getTagsLength());
    }
  }

  @Override
  public void init(final Map<String, String> options) throws IOException {
    final String columnStr = getColumnOptionValue(options);

    if (columnStr.length() == 0) {
      throw new IllegalArgumentException("The column must not be empty");
    }
    columnFamilyIds =
        Sets.newHashSet(
            Iterables.transform(
                Splitter.on(",").split(columnStr),
                new Function<String, GeoWaveColumnId>() {

                  @Override
                  public GeoWaveColumnId apply(final String input) {
                    return new ByteArrayColumnId(new ByteArray(input));
                  }
                }));
  }

  protected String getColumnOptionValue(final Map<String, String> options) {
    // if this is not "row" merging than it is merging stats on the metadata
    // table
    return MetadataType.STATISTIC_VALUES.id();
  }

  @Override
  public void preScannerOpen(final Scan scan) {
    final int maxVersions = scan.getMaxVersions();
    if ((maxVersions > 0) && (maxVersions < Integer.MAX_VALUE)) {
      scan.setAttribute(OLD_MAX_VERSIONS_KEY, ByteBuffer.allocate(4).putInt(maxVersions).array());
    }
    scan.setMaxVersions();
  }

  @Override
  public byte[] toBinary() {
    return new byte[] {};
  }

  @Override
  public void fromBinary(final byte[] bytes) {}

  /**
   * The timestamps that the delete markers of a row mask, per column family and per column. A
   * family marker has an empty qualifier, so it masks Puts of every column in its family.
   */
  private static class RowDeletes {
    private final byte[] row;
    private final Map<ByteArray, MaskedTimestamps> families = new HashMap<>();
    private final Map<PartialCellEquality, MaskedTimestamps> columns = new HashMap<>();

    private RowDeletes(final byte[] row) {
      this.row = row;
    }

    private void add(final Cell marker) {
      final long timestamp = marker.getTimestamp();
      switch (marker.getType()) {
        case DeleteFamily:
          family(marker).maskUpTo(timestamp);
          break;
        case DeleteFamilyVersion:
          family(marker).mask(timestamp);
          break;
        case DeleteColumn:
          column(marker).maskUpTo(timestamp);
          break;
        case Delete:
          column(marker).mask(timestamp);
          break;
        default:
          break;
      }
    }

    private MaskedTimestamps family(final Cell marker) {
      return families.computeIfAbsent(
          new ByteArray(CellUtil.cloneFamily(marker)),
          k -> new MaskedTimestamps());
    }

    private MaskedTimestamps column(final Cell marker) {
      // the key can outlive the batch of cells the marker came in, so it holds a copy
      return columns.computeIfAbsent(
          new PartialCellEquality(new KeyValue(marker), false),
          k -> new MaskedTimestamps());
    }

    private boolean masks(final Cell put) {
      final long timestamp = put.getTimestamp();
      if (!families.isEmpty()) {
        final MaskedTimestamps family = families.get(new ByteArray(CellUtil.cloneFamily(put)));
        if ((family != null) && family.contains(timestamp)) {
          return true;
        }
      }
      if (!columns.isEmpty()) {
        final MaskedTimestamps column = columns.get(new PartialCellEquality(put, false));
        return (column != null) && column.contains(timestamp);
      }
      return false;
    }
  }

  private static class MaskedTimestamps {
    private long upTo = Long.MIN_VALUE;
    private final Set<Long> versions = new HashSet<>();

    private void maskUpTo(final long timestamp) {
      upTo = Math.max(upTo, timestamp);
    }

    private void mask(final long timestamp) {
      versions.add(timestamp);
    }

    private boolean contains(final long timestamp) {
      return (timestamp <= upTo) || versions.contains(timestamp);
    }
  }
}
