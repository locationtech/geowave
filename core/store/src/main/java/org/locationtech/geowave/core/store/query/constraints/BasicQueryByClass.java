/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.constraints;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.numeric.BasicNumericDataset;
import org.locationtech.geowave.core.index.numeric.MultiDimensionalNumericData;
import org.locationtech.geowave.core.index.numeric.NumericData;
import org.locationtech.geowave.core.index.numeric.NumericRange;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.query.filter.BasicQueryFilter.BasicQueryCompareOperation;
import org.locationtech.geowave.core.store.query.filter.FilterList;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.math.DoubleMath;

/**
 * The Basic Query class represent a hyper-cube(s) query across all dimensions that match the
 * Constraints passed into the constructor
 *
 * <p> NOTE: query to an index that requires a constraint and the constraint is missing within the
 * query equates to an unconstrained index scan. The query filter is still applied.
 */
public class BasicQueryByClass extends BasicQuery {
  private static final double DOUBLE_TOLERANCE = 1E-12d;
  private static final Logger LOGGER = LoggerFactory.getLogger(BasicQueryByClass.class);

  /** A set of constraints, one range per dimension */
  public static class ConstraintSet {
    protected Map<Class<? extends NumericDimensionDefinition>, ConstraintData> constraintsPerTypeOfDimensionDefinition;

    public ConstraintSet() {
      constraintsPerTypeOfDimensionDefinition = new HashMap<>();
    }

    public ConstraintSet(
        final Map<Class<? extends NumericDimensionDefinition>, ConstraintData> constraintsPerTypeOfDimensionDefinition) {
      this.constraintsPerTypeOfDimensionDefinition = constraintsPerTypeOfDimensionDefinition;
    }

    public ConstraintSet(
        final Class<? extends NumericDimensionDefinition> dimDefinition,
        final ConstraintData constraintData) {
      this();
      addConstraint(dimDefinition, constraintData);
    }

    public ConstraintSet(
        final ConstraintData constraintData,
        final Class<? extends NumericDimensionDefinition>... dimDefinitions) {
      this();
      for (final Class<? extends NumericDimensionDefinition> dimDefinition : dimDefinitions) {
        addConstraint(dimDefinition, constraintData);
      }
    }

    public void addConstraint(
        final Class<? extends NumericDimensionDefinition> dimDefinition,
        final ConstraintData constraintData) {
      final ConstraintData myCd = constraintsPerTypeOfDimensionDefinition.get(dimDefinition);
      if (myCd != null) {
        constraintsPerTypeOfDimensionDefinition.put(dimDefinition, myCd.merge(constraintData));
      } else {
        constraintsPerTypeOfDimensionDefinition.put(dimDefinition, constraintData);
      }
    }

    public ConstraintSet merge(final ConstraintSet constraintSet) {
      final Map<Class<? extends NumericDimensionDefinition>, ConstraintData> newSet =
          new HashMap<>();

      for (final Map.Entry<Class<? extends NumericDimensionDefinition>, ConstraintData> entry : constraintSet.constraintsPerTypeOfDimensionDefinition.entrySet()) {
        final ConstraintData data = constraintsPerTypeOfDimensionDefinition.get(entry.getKey());

        if (data == null) {
          newSet.put(entry.getKey(), entry.getValue());
        } else {
          newSet.put(entry.getKey(), data.merge(entry.getValue()));
        }
      }
      for (final Map.Entry<Class<? extends NumericDimensionDefinition>, ConstraintData> entry : constraintsPerTypeOfDimensionDefinition.entrySet()) {
        final ConstraintData data =
            constraintSet.constraintsPerTypeOfDimensionDefinition.get(entry.getKey());

        if (data == null) {
          newSet.put(entry.getKey(), entry.getValue());
        }
      }
      return new ConstraintSet(newSet);
    }

    public boolean isEmpty() {
      return constraintsPerTypeOfDimensionDefinition.isEmpty();
    }

    public boolean matches(final ConstraintSet constraints) {
      if (constraints.isEmpty() != isEmpty()) {
        return false;
      }
      for (final Map.Entry<Class<? extends NumericDimensionDefinition>, ConstraintData> entry : constraintsPerTypeOfDimensionDefinition.entrySet()) {
        final ConstraintData data =
            constraints.constraintsPerTypeOfDimensionDefinition.get(entry.getKey());
        if ((data == null) || !data.matches(entry.getValue())) {
          return false;
        }
      }
      return true;
    }

    /*
     * Makes the decision to provide a empty data set if an one dimension is left unconstrained.
     */
    public MultiDimensionalNumericData getIndexConstraints(
        final NumericIndexStrategy indexStrategy) {
      if (constraintsPerTypeOfDimensionDefinition.isEmpty()) {
        return new BasicNumericDataset();
      }
      final NumericDimensionDefinition[] dimensionDefinitions =
          indexStrategy.getOrderedDimensionDefinitions();
      final NumericData[] dataPerDimension = new NumericData[dimensionDefinitions.length];
      // all or nothing...for now
      for (int d = 0; d < dimensionDefinitions.length; d++) {
        final ConstraintData dimConstraint =
            constraintsPerTypeOfDimensionDefinition.get(dimensionDefinitions[d].getClass());
        if (dimConstraint == null) {
          return new BasicNumericDataset();
        }
        dataPerDimension[d] = dimConstraint.range;
      }
      return new BasicNumericDataset(dataPerDimension);
    }

    protected QueryFilter createFilter(final Index index, final BasicQuery basicQuery) {
      final CommonIndexModel indexModel = index.getIndexModel();
      final NumericDimensionField<?>[] dimensionFields = indexModel.getDimensions();
      NumericDimensionField<?>[] orderedConstrainedDimensionFields = dimensionFields;
      NumericDimensionField<?>[] unconstrainedDimensionFields;
      NumericData[] orderedConstraintsPerDimension = new NumericData[dimensionFields.length];
      // trim dimension fields to be only what is contained in the
      // constraints
      final Set<Integer> fieldsToTrim = new HashSet<>();
      for (int d = 0; d < dimensionFields.length; d++) {
        final ConstraintData nd =
            constraintsPerTypeOfDimensionDefinition.get(
                dimensionFields[d].getBaseDefinition().getClass());
        if (nd == null) {
          fieldsToTrim.add(d);
        } else {
          orderedConstraintsPerDimension[d] =
              constraintsPerTypeOfDimensionDefinition.get(
                  dimensionFields[d].getBaseDefinition().getClass()).range;
        }
      }
      if (!fieldsToTrim.isEmpty()) {
        final NumericDimensionField<?>[] newDimensionFields =
            new NumericDimensionField[dimensionFields.length - fieldsToTrim.size()];

        unconstrainedDimensionFields = new NumericDimensionField[fieldsToTrim.size()];
        final NumericData[] newOrderedConstraintsPerDimension =
            new NumericData[newDimensionFields.length];
        int newDimensionCtr = 0;
        int constrainedCtr = 0;
        for (int i = 0; i < dimensionFields.length; i++) {
          if (!fieldsToTrim.contains(i)) {
            newDimensionFields[newDimensionCtr] = dimensionFields[i];
            newOrderedConstraintsPerDimension[newDimensionCtr++] =
                orderedConstraintsPerDimension[i];
          } else {
            unconstrainedDimensionFields[constrainedCtr++] = dimensionFields[i];
          }
        }
        orderedConstrainedDimensionFields = newDimensionFields;
        orderedConstraintsPerDimension = newOrderedConstraintsPerDimension;
      } else {
        unconstrainedDimensionFields = new NumericDimensionField[] {};
      }
      return basicQuery.createQueryFilter(
          new BasicNumericDataset(orderedConstraintsPerDimension),
          orderedConstrainedDimensionFields,
          unconstrainedDimensionFields,
          index);
    }

    public byte[] toBinary() {
      final List<byte[]> bytes = new ArrayList<>(constraintsPerTypeOfDimensionDefinition.size());
      int totalBytes = VarintUtils.unsignedIntByteLength(bytes.size());
      for (final Entry<Class<? extends NumericDimensionDefinition>, ConstraintData> c : constraintsPerTypeOfDimensionDefinition.entrySet()) {
        final byte[] className = StringUtils.stringToBinary(c.getKey().getName());
        final double min = c.getValue().range.getMin();
        final double max = c.getValue().range.getMax();
        final int entryLength =
            className.length + 17 + VarintUtils.unsignedIntByteLength(className.length);
        final byte isDefault = (byte) (c.getValue().isDefault ? 1 : 0);
        final ByteBuffer entryBuf = ByteBuffer.allocate(entryLength);
        VarintUtils.writeUnsignedInt(className.length, entryBuf);
        entryBuf.put(className);
        entryBuf.putDouble(min);
        entryBuf.putDouble(max);
        entryBuf.put(isDefault);
        bytes.add(entryBuf.array());
        totalBytes += entryLength;
      }

      final ByteBuffer buf = ByteBuffer.allocate(totalBytes);
      VarintUtils.writeUnsignedInt(bytes.size(), buf);
      for (final byte[] entryBytes : bytes) {
        buf.put(entryBytes);
      }
      return buf.array();
    }

    public void fromBinary(final byte[] bytes) {
      final ByteBuffer buf = ByteBuffer.wrap(bytes);
      final int numEntries = VarintUtils.readUnsignedInt(buf);
      final Map<Class<? extends NumericDimensionDefinition>, ConstraintData> constraintsPerTypeOfDimensionDefinition =
          new HashMap<>(numEntries);
      for (int i = 0; i < numEntries; i++) {
        final int classNameLength = VarintUtils.readUnsignedInt(buf);
        final byte[] className = ByteArrayUtils.safeRead(buf, classNameLength);
        final double min = buf.getDouble();
        final double max = buf.getDouble();
        final boolean isDefault = buf.get() > 0;
        final String classNameStr = StringUtils.stringFromBinary(className);
        try {
          final Class<? extends NumericDimensionDefinition> cls =
              (Class<? extends NumericDimensionDefinition>) Class.forName(classNameStr);
          constraintsPerTypeOfDimensionDefinition.put(
              cls,
              new ConstraintData(new NumericRange(min, max), isDefault));
        } catch (final ClassNotFoundException e) {
          // HP Fortify "Improper Output Neutralization" false
          // positive
          // What Fortify considers "user input" comes only
          // from users with OS-level access anyway
          LOGGER.warn("Cannot find dimension definition class: " + classNameStr, e);
        }
      }
      this.constraintsPerTypeOfDimensionDefinition = constraintsPerTypeOfDimensionDefinition;
    }
  }

  public static class ConstraintData {
    protected NumericData range;
    protected boolean isDefault;

    public ConstraintData(final NumericData range, final boolean isDefault) {
      super();
      this.range = range;
      this.isDefault = isDefault;
    }

    public boolean intersects(final ConstraintData cd) {
      final double i1 = cd.range.getMin();
      final double i2 = cd.range.getMax();
      final double j1 = range.getMin();
      final double j2 = range.getMax();
      return ((i1 < j2) || DoubleMath.fuzzyEquals(i1, j2, DOUBLE_TOLERANCE))
          && ((i2 > j1) || DoubleMath.fuzzyEquals(i2, j1, DOUBLE_TOLERANCE));
    }

    public ConstraintData merge(final ConstraintData cd) {
      if (range.equals(cd.range)) {
        return new ConstraintData(range, isDefault);
      }
      return new ConstraintData(
          new NumericRange(
              Math.min(cd.range.getMin(), range.getMin()),
              Math.max(cd.range.getMax(), range.getMax())),
          false); // TODO: ideally, this would be set
      // based on some
      // logic
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = (prime * result) + (isDefault ? 1231 : 1237);
      result = (prime * result) + ((range == null) ? 0 : range.hashCode());
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
      final ConstraintData other = (ConstraintData) obj;
      if (isDefault != other.isDefault) {
        return false;
      }
      if (range == null) {
        if (other.range != null) {
          return false;
        }
      } else if (!range.equals(other.range)) {
        return false;
      }
      return true;
    }

    /**
     * Ignores 'default' indicator
     *
     * @param other
     * @return {@code true} if these constraints match the other constraints
     */
    public boolean matches(final ConstraintData other) {
      if (this == other) {
        return true;
      }

      if (range == null) {
        if (other.range != null) {
          return false;
        }
      } else if (!DoubleMath.fuzzyEquals(range.getMin(), other.range.getMin(), DOUBLE_TOLERANCE)
          || !DoubleMath.fuzzyEquals(range.getMax(), other.range.getMax(), DOUBLE_TOLERANCE)) {
        return false;
      }
      return true;
    }
  }

  /** A list of Constraint Sets. Each Constraint Set is an individual hyper-cube query. */
  public static class ConstraintsByClass implements Constraints {
    // these basic queries are tied to NumericDimensionDefinition types, not
    // ideal, but third-parties can and will nned to implement their own
    // queries if they implement their own dimension definitions
    protected List<ConstraintSet> constraintsSets = new LinkedList<>();

    public ConstraintsByClass() {}

    public ConstraintsByClass(final ConstraintSet constraintSet) {
      constraintsSets.add(constraintSet);
    }

    public ConstraintsByClass(final List<ConstraintSet> constraintSets) {
      constraintsSets.addAll(constraintSets);
    }

    public ConstraintsByClass merge(final ConstraintsByClass constraints) {
      return merge(constraints.constraintsSets);
    }

    public ConstraintsByClass merge(final List<ConstraintSet> otherConstraintSets) {

      if (otherConstraintSets.isEmpty()) {
        return this;
      } else if (isEmpty()) {
        return new ConstraintsByClass(otherConstraintSets);
      }
      final List<ConstraintSet> newSets = new LinkedList<>();

      for (final ConstraintSet newSet : otherConstraintSets) {
        add(newSets, constraintsSets, newSet);
      }
      return new ConstraintsByClass(newSets);
    }

    private static void add(
        final List<ConstraintSet> newSets,
        final List<ConstraintSet> currentSets,
        final ConstraintSet newSet) {
      for (final ConstraintSet cs : currentSets) {
        newSets.add(cs.merge(newSet));
      }
    }

    public boolean isEmpty() {
      return constraintsSets.isEmpty();
    }

    public boolean matches(final ConstraintsByClass constraints) {
      if (constraints.isEmpty() != isEmpty()) {
        return false;
      }
      for (final ConstraintSet set : constraintsSets) {
        boolean foundMatch = false;
        for (final ConstraintSet otherSet : constraints.constraintsSets) {
          foundMatch |= set.matches(otherSet);
        }
        if (!foundMatch) {
          return false;
        }
      }
      return true;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = (prime * result) + ((constraintsSets == null) ? 0 : constraintsSets.hashCode());
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
      final ConstraintsByClass other = (ConstraintsByClass) obj;
      if (constraintsSets == null) {
        if (other.constraintsSets != null) {
          return false;
        }
      } else if (!constraintsSets.equals(other.constraintsSets)) {
        return false;
      }
      return true;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.locationtech.geowave.core.store.query.constraints.Constraints#getIndexConstraints(org.
     * locationtech.geowave.core.index.NumericIndexStrategy)
     */
    @Override
    public List<MultiDimensionalNumericData> getIndexConstraints(final Index index) {
      final NumericIndexStrategy indexStrategy = index.getIndexStrategy();
      if (constraintsSets.isEmpty()) {
        return Collections.emptyList();
      }
      final List<MultiDimensionalNumericData> setRanges = new ArrayList<>(constraintsSets.size());
      for (final ConstraintSet set : constraintsSets) {
        final MultiDimensionalNumericData mdSet = set.getIndexConstraints(indexStrategy);
        if (!mdSet.isEmpty()) {
          setRanges.add(mdSet);
        }
      }
      return setRanges;
    }

    @Override
    public byte[] toBinary() {
      final List<byte[]> bytes = new ArrayList<>(constraintsSets.size());
      int totalBytes = 0;
      for (final ConstraintSet c : constraintsSets) {
        bytes.add(c.toBinary());
        final int length = bytes.get(bytes.size() - 1).length;
        totalBytes += (length + VarintUtils.unsignedIntByteLength(length));
      }

      final ByteBuffer buf =
          ByteBuffer.allocate(totalBytes + VarintUtils.unsignedIntByteLength(bytes.size()));
      VarintUtils.writeUnsignedInt(bytes.size(), buf);
      for (final byte[] entryBytes : bytes) {
        VarintUtils.writeUnsignedInt(entryBytes.length, buf);
        buf.put(entryBytes);
      }
      return buf.array();
    }

    @Override
    public void fromBinary(final byte[] bytes) {
      final ByteBuffer buf = ByteBuffer.wrap(bytes);
      final int numEntries = VarintUtils.readUnsignedInt(buf);
      final List<ConstraintSet> sets = new LinkedList<>();
      for (int i = 0; i < numEntries; i++) {
        final byte[] d = ByteArrayUtils.safeRead(buf, VarintUtils.readUnsignedInt(buf));
        final ConstraintSet cs = new ConstraintSet();
        cs.fromBinary(d);
        sets.add(cs);
      }
      constraintsSets = sets;
    }

    @Override
    public List<QueryFilter> createFilters(final Index index, final BasicQuery parentQuery) {
      final List<QueryFilter> filters = new ArrayList<>();
      for (final ConstraintSet constraint : constraintsSets) {
        final QueryFilter filter = constraint.createFilter(index, parentQuery);
        if (filter != null) {
          filters.add(filter);
        }
      }
      if (!filters.isEmpty()) {
        return Collections.<QueryFilter>singletonList(
            filters.size() == 1 ? filters.get(0) : new FilterList(false, filters));
      }
      return Collections.emptyList();
    }
  }

  // this is a clientside flag that is unnecessary to persist
  protected transient boolean exact = true;

  public BasicQueryByClass() {}

  public BasicQueryByClass(final ConstraintsByClass constraints) {
    super(constraints);
  }


  public BasicQueryByClass(
      final ConstraintsByClass constraints,
      final BasicQueryCompareOperation compareOp) {
    super(constraints, compareOp);
  }

  @Override
  public byte[] toBinary() {
    return constraints.toBinary();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    constraints = new ConstraintsByClass();
    constraints.fromBinary(bytes);
  }

  public boolean isExact() {
    return exact;
  }

  public void setExact(final boolean exact) {
    this.exact = exact;
  }
}
