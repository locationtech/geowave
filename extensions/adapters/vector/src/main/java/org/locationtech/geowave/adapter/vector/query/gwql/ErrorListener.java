package org.locationtech.geowave.adapter.vector.query.gwql;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

/**
 * Error listener that wraps ANTLR syntax errors in our own exception class.
 */
public class ErrorListener extends BaseErrorListener {
  @Override
  public void syntaxError(
      Recognizer<?, ?> recognizer,
      Object offendingSymbol,
      int line,
      int position,
      String message,
      RecognitionException e) throws GWQLParseException {
    throw new GWQLParseException(line, position, message.replace(" K_", " "));
  }
}
