package org.locationtech.geowave.datastore.kudu;

import org.apache.kudu.client.PartialRow;

public interface PersistentKuduRow {
  /**
   * Populate the given {@link PartialRow} with all fields from this row
   *
   * @param partialRow
   */
  void populatePartialRow(PartialRow partialRow);

  /**
   * Populate the given {@link PartialRow} with fields from this row comprising the primary key
   *
   * @param partialRow
   */
  void populatePartialRowPrimaryKey(PartialRow partialRow);
}
