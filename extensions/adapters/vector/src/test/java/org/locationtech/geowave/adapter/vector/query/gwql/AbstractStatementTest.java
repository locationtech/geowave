package org.locationtech.geowave.adapter.vector.query.gwql;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.locationtech.geowave.adapter.vector.query.gwql.parse.GWQLParser;

public abstract class AbstractStatementTest {
  protected void assertInvalidStatement(String statement, String expectedMessage) {
    try {
      GWQLParser.parseStatement(statement);
      fail();
    } catch (GWQLParseException e) {
      // expected
      assertTrue(
          e.getMessage() + " does not contain " + expectedMessage,
          e.getMessage().contains(expectedMessage));
    }
  }
}
