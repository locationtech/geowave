/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression;

/**
 * A field value implementation for any field value.
 */
public class GenericFieldValue extends FieldValue<Object> implements GenericExpression {

  public GenericFieldValue() {}

  public GenericFieldValue(final String fieldName) {
    super(fieldName);
  }

  @Override
  protected Object evaluateValueInternal(final Object value) {
    return value;
  }

  public static GenericFieldValue of(final String fieldName) {
    return new GenericFieldValue(fieldName);
  }
}
