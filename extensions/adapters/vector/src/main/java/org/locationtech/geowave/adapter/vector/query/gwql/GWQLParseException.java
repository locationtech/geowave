package org.locationtech.geowave.adapter.vector.query.gwql;

import org.antlr.v4.runtime.misc.ParseCancellationException;

/**
 * Exception class for syntax errors in the query language.
 */
public class GWQLParseException extends ParseCancellationException {
  private static final long serialVersionUID = 1L;

  public GWQLParseException(int line, int position, String message) {
    super("Invalid Syntax: " + message + " at [" + line + ":" + position + "]");
  }

}
