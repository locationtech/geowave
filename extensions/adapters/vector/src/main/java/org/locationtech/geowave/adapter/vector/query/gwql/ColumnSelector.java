package org.locationtech.geowave.adapter.vector.query.gwql;

/**
 * Selector that pulls a value from a single column of the results set.
 */
public class ColumnSelector extends Selector {

  private final String columnName;

  /**
   * @param columnName the column to select
   */
  public ColumnSelector(final String columnName) {
    this(columnName, null);
  }

  /**
   * @param columnName the column to select
   * @param alias the alias of the column
   */
  public ColumnSelector(final String columnName, final String alias) {
    super(SelectorType.SIMPLE, alias);
    this.columnName = columnName;
  }

  /**
   * @return the selected column name
   */
  public String columnName() {
    return columnName;
  }

  /**
   * @return the display name of this selector
   */
  @Override
  public String selectorName() {
    return columnName;
  }

}
