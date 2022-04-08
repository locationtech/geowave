/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.gwql;

import org.antlr.v4.runtime.misc.ParseCancellationException;

/**
 * Exception class for syntax errors in the query language.
 */
public class GWQLParseException extends ParseCancellationException {
  private static final long serialVersionUID = 1L;

  public GWQLParseException(final String message) {
    super(message);
  }

  public GWQLParseException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public GWQLParseException(int line, int position, String message) {
    super("Invalid Syntax: " + message + " at [" + line + ":" + position + "]");
  }

}
