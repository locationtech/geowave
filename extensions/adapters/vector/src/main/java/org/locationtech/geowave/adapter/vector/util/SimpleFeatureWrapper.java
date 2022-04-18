/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.util;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.locationtech.geowave.core.index.ByteArray;
import org.opengis.feature.GeometryAttribute;
import org.opengis.feature.IllegalAttributeException;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.Name;
import org.opengis.filter.identity.FeatureId;
import org.opengis.geometry.BoundingBox;

public class SimpleFeatureWrapper implements SimpleFeature {
  private final SimpleFeature simpleFeature;
  private final ByteArray insertionId;
  private final int subStratIdx;

  public SimpleFeatureWrapper(
      final SimpleFeature simpleFeature,
      final ByteArray insertionId,
      final int subStratIdx) {
    this.simpleFeature = simpleFeature;
    this.insertionId = insertionId;
    this.subStratIdx = subStratIdx;
  }

  public SimpleFeature getSimpleFeature() {
    return simpleFeature;
  }

  public ByteArray getInsertionId() {
    return insertionId;
  }

  public int getSubStratIdx() {
    return subStratIdx;
  }

  @Override
  public FeatureId getIdentifier() {
    return simpleFeature.getIdentifier();
  }

  @Override
  public AttributeDescriptor getDescriptor() {
    return simpleFeature.getDescriptor();
  }

  @Override
  public BoundingBox getBounds() {
    return simpleFeature.getBounds();
  }

  @Override
  public String getID() {
    return simpleFeature.getID();
  }

  @Override
  public SimpleFeatureType getType() {
    return simpleFeature.getType();
  }

  @Override
  public SimpleFeatureType getFeatureType() {
    return simpleFeature.getFeatureType();
  }

  @Override
  public void setValue(final Object newValue) {
    simpleFeature.setValue(newValue);
  }

  @Override
  public List<Object> getAttributes() {
    return simpleFeature.getAttributes();
  }

  @Override
  public GeometryAttribute getDefaultGeometryProperty() {
    return simpleFeature.getDefaultGeometryProperty();
  }

  @Override
  public void setValue(final Collection<Property> values) {
    simpleFeature.setValue(values);
  }

  @Override
  public void setAttributes(final List<Object> values) {
    simpleFeature.setAttributes(values);
  }

  @Override
  public void setDefaultGeometryProperty(final GeometryAttribute geometryAttribute) {
    simpleFeature.setDefaultGeometryProperty(geometryAttribute);
  }

  @Override
  public Collection<? extends Property> getValue() {
    return simpleFeature.getValue();
  }

  @Override
  public Collection<Property> getProperties(final Name name) {
    return simpleFeature.getProperties(name);
  }

  @Override
  public void setAttributes(final Object[] values) {
    simpleFeature.setAttributes(values);
  }

  @Override
  public Name getName() {
    return simpleFeature.getName();
  }

  @Override
  public Property getProperty(final Name name) {
    return simpleFeature.getProperty(name);
  }

  @Override
  public Object getAttribute(final String name) {
    return simpleFeature.getAttribute(name);
  }

  @Override
  public boolean isNillable() {
    return simpleFeature.isNillable();
  }

  @Override
  public Map<Object, Object> getUserData() {
    return simpleFeature.getUserData();
  }

  @Override
  public void setAttribute(final String name, final Object value) {
    simpleFeature.setAttribute(name, value);
  }

  @Override
  public Collection<Property> getProperties(final String name) {
    return simpleFeature.getProperties(name);
  }

  @Override
  public Object getAttribute(final Name name) {
    return simpleFeature.getAttribute(name);
  }

  @Override
  public void setAttribute(final Name name, final Object value) {
    simpleFeature.setAttribute(name, value);
  }

  @Override
  public Collection<Property> getProperties() {
    return simpleFeature.getProperties();
  }

  @Override
  public Property getProperty(final String name) {
    return simpleFeature.getProperty(name);
  }

  @Override
  public Object getAttribute(final int index) throws IndexOutOfBoundsException {
    return simpleFeature.getAttribute(index);
  }

  @Override
  public void setAttribute(final int index, final Object value) throws IndexOutOfBoundsException {
    simpleFeature.setAttribute(index, value);
  }

  @Override
  public void validate() throws IllegalAttributeException {
    simpleFeature.validate();
  }

  @Override
  public int getAttributeCount() {
    return simpleFeature.getAttributeCount();
  }

  @Override
  public Object getDefaultGeometry() {
    return simpleFeature.getDefaultGeometry();
  }

  @Override
  public void setDefaultGeometry(final Object geometry) {
    simpleFeature.setDefaultGeometry(geometry);
  }
}
