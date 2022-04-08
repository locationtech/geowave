/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression;

/**
 * Thrown when an invalid filter is made, such as creating a literal with an incompatible object.
 */
public class InvalidFilterException extends RuntimeException {

  private static final long serialVersionUID = -2922956287189544264L;

  public InvalidFilterException(final String message) {
    super(message);
  }

  public InvalidFilterException(final String message, final Throwable cause) {
    super(message, cause);
  }

}
