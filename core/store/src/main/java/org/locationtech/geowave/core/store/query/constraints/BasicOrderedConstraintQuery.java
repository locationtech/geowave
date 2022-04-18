/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.constraints;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.Range;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.numeric.BasicNumericDataset;
import org.locationtech.geowave.core.index.numeric.MultiDimensionalNumericData;
import org.locationtech.geowave.core.index.numeric.NumericData;
import org.locationtech.geowave.core.index.numeric.NumericRange;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.query.filter.BasicQueryFilter.BasicQueryCompareOperation;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

public class BasicOrderedConstraintQuery extends BasicQuery {

  /** A list of Constraint Sets. Each Constraint Set is an individual hyper-cube query. */
  public static class OrderedConstraints implements Constraints {
    private Range<Double>[] rangesPerDimension;
    private String indexName;

    public OrderedConstraints() {}

    public OrderedConstraints(final Range<Double> rangePerDimension) {
      this(new Range[] {rangePerDimension}, null);
    }

    public OrderedConstraints(final Range<Double>[] rangesPerDimension) {
      this(rangesPerDimension, null);
    }

    public OrderedConstraints(final Range<Double>[] rangesPerDimension, final String indexName) {
      this.rangesPerDimension = rangesPerDimension;
      this.indexName = indexName;
    }

    @Override
    public byte[] toBinary() {
      final byte[] indexNameBinary;
      if (indexName != null) {
        indexNameBinary = StringUtils.stringToBinary(indexName);
      } else {
        indexNameBinary = new byte[0];
      }
      final ByteBuffer buf =
          ByteBuffer.allocate(
              VarintUtils.unsignedIntByteLength(rangesPerDimension.length)
                  + VarintUtils.unsignedIntByteLength(indexNameBinary.length)
                  + (16 * rangesPerDimension.length)
                  + indexNameBinary.length);
      VarintUtils.writeUnsignedInt(rangesPerDimension.length, buf);
      VarintUtils.writeUnsignedInt(indexNameBinary.length, buf);
      for (int i = 0; i < rangesPerDimension.length; i++) {
        buf.putDouble(rangesPerDimension[i].getMinimum());
        buf.putDouble(rangesPerDimension[i].getMaximum());
      }
      buf.put(indexNameBinary);
      return buf.array();
    }

    @Override
    public void fromBinary(final byte[] bytes) {
      final ByteBuffer buf = ByteBuffer.wrap(bytes);
      final int numRanges = VarintUtils.readUnsignedInt(buf);
      ByteArrayUtils.verifyBufferSize(buf, numRanges);
      rangesPerDimension = new Range[numRanges];
      final int indexNameBinaryLength = VarintUtils.readUnsignedInt(buf);
      for (int i = 0; i < rangesPerDimension.length; i++) {
        rangesPerDimension[i] = Range.between(buf.getDouble(), buf.getDouble());
      }
      if (indexNameBinaryLength > 0) {
        final byte[] indexNameBinary = ByteArrayUtils.safeRead(buf, indexNameBinaryLength);
        indexName = StringUtils.stringFromBinary(indexNameBinary);
      } else {
        indexName = null;
      }
    }

    @Override
    public List<MultiDimensionalNumericData> getIndexConstraints(final Index index) {
      if (((indexName == null) || indexName.equals(index.getName()))
          && (index.getIndexStrategy().getOrderedDimensionDefinitions().length == rangesPerDimension.length)) {
        return Collections.singletonList(getIndexConstraints());
      }
      return Collections.emptyList();
    }

    protected MultiDimensionalNumericData getIndexConstraints() {
      return new BasicNumericDataset(
          Arrays.stream(rangesPerDimension).map(
              r -> new NumericRange(r.getMinimum(), r.getMaximum())).toArray(
                  i -> new NumericData[i]));
    }

    @Override
    public List<QueryFilter> createFilters(final Index index, final BasicQuery parentQuery) {
      final QueryFilter filter =
          parentQuery.createQueryFilter(
              getIndexConstraints(),
              index.getIndexModel().getDimensions(),
              new NumericDimensionField[0],
              index);
      if (filter != null) {
        return Collections.singletonList(filter);
      }
      return Collections.emptyList();
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = (prime * result) + ((indexName == null) ? 0 : indexName.hashCode());
      result = (prime * result) + Arrays.hashCode(rangesPerDimension);
      return result;
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
      final OrderedConstraints other = (OrderedConstraints) obj;
      if (indexName == null) {
        if (other.indexName != null) {
          return false;
        }
      } else if (!indexName.equals(other.indexName)) {
        return false;
      }
      if (!Arrays.equals(rangesPerDimension, other.rangesPerDimension)) {
        return false;
      }
      return true;
    }
  }

  public BasicOrderedConstraintQuery() {}

  public BasicOrderedConstraintQuery(final OrderedConstraints constraints) {
    super(constraints);
  }


  public BasicOrderedConstraintQuery(
      final OrderedConstraints constraints,
      final BasicQueryCompareOperation compareOp) {
    super(constraints, compareOp);
  }

  @Override
  public byte[] toBinary() {
    return constraints.toBinary();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    constraints = new OrderedConstraints();
    constraints.fromBinary(bytes);
  }

  @Override
  public boolean indexMustBeSpecified() {
    return true;
  }
}
