/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression.text;

import org.locationtech.geowave.core.store.query.filter.expression.FieldValue;

/**
 * A field value implementation for string adapter fields.
 */
public class TextFieldValue extends FieldValue<String> implements TextExpression {

  public TextFieldValue() {}

  public TextFieldValue(final String fieldName) {
    super(fieldName);
  }

  public static TextFieldValue of(final String fieldName) {
    return new TextFieldValue(fieldName);
  }

  @Override
  protected String evaluateValueInternal(final Object value) {
    return value.toString();
  }

}
