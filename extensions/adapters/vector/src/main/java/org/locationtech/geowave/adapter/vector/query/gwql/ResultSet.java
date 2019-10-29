package org.locationtech.geowave.adapter.vector.query.gwql;

import org.locationtech.geowave.core.store.CloseableIterator;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/**
 * Interface for a set of results from a GeoWave query.
 */
public interface ResultSet extends CloseableIterator<Result> {

  /**
   * @return the number of columns that each result contains
   */
  public int columnCount();

  /**
   * @param index the index of the column
   * @return the display name of the column at the given index
   */
  public String columnName(final int index);

  /**
   * @param index the index of the column
   * @return the Class of the objects that can be found in the given column
   */
  public Class<?> columnType(final int index);

  /**
   * @return the coordinate reference system of the geometries in this result set
   */
  public CoordinateReferenceSystem getCRS();
}
