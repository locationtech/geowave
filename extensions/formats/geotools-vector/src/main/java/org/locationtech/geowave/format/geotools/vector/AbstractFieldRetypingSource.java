/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.geotools.vector;

import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.identity.FeatureIdImpl;
import org.locationtech.geowave.format.geotools.vector.RetypingVectorDataPlugin.RetypingVectorDataSource;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.Name;
import org.opengis.filter.identity.FeatureId;

public abstract class AbstractFieldRetypingSource implements RetypingVectorDataSource {

  public abstract String getFeatureId(SimpleFeature original);

  public abstract Object retypeAttributeValue(Object value, Name attributeName);

  @Override
  public SimpleFeature getRetypedSimpleFeature(
      final SimpleFeatureBuilder builder,
      final SimpleFeature original) {

    final SimpleFeatureType target = builder.getFeatureType();
    for (int i = 0; i < target.getAttributeCount(); i++) {
      final AttributeDescriptor attributeType = target.getDescriptor(i);
      Object value = null;

      if (original.getFeatureType().getDescriptor(attributeType.getName()) != null) {
        final Name name = attributeType.getName();
        value = retypeAttributeValue(original.getAttribute(name), name);
      }

      builder.add(value);
    }
    String featureId = getFeatureId(original);
    if (featureId == null) {
      final FeatureId id =
          getDefaultFeatureId(original.getIdentifier(), original.getFeatureType(), target);
      featureId = id.getID();
    }
    final SimpleFeature retyped = builder.buildFeature(featureId);
    retyped.getUserData().putAll(original.getUserData());
    return retyped;
  }

  private static FeatureId getDefaultFeatureId(
      final FeatureId sourceId,
      final SimpleFeatureType original,
      final SimpleFeatureType target) {
    // a null ID will default to use the original
    final String originalTypeName = original.getName().getLocalPart();
    final String destTypeName = target.getName().getLocalPart();
    if (destTypeName.equals(originalTypeName)) {
      return sourceId;
    }

    final String prefix = originalTypeName + ".";
    if (sourceId.getID().startsWith(prefix)) {
      return new FeatureIdImpl(destTypeName + "." + sourceId.getID().substring(prefix.length()));
    } else {
      return sourceId;
    }
  }
}
