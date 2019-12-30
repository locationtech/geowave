package org.locationtech.geowave.core.cli.utils;

import java.util.Comparator;
import java.util.List;

/**
 * Performs a StringValue comparison of only the first element of equal-sized Lists
 * of Objects, and trivial sorting rules for lists.
 */
public class FirstElementListComparator implements Comparator<List<Object>> {

  @Override
  public int compare(List<Object> listThis, List<Object> listOther) {
    int rc = 0;

    if (listThis == null && listOther == null) { // Consider both null as "equal"
      rc = 0;
    } else if (listThis == null && listOther != null) { // null sorts ahead of non-null
      rc = -1;
    } else if (listThis != null && listOther == null) { // null sorts ahead of non-null
      rc = 1;
    } else if (listThis.size() != listOther.size()) { // shorter list ahead of longer list
      rc = listThis.size() - listOther.size();
    } else if (listThis.size() > 0) { // lists are equal size and not zero-length
      String strThis = String.valueOf(listThis.get(0) == null ? "" : listThis.get(0));
      String strOther = String.valueOf(listOther.get(0) == null ? "" : listOther.get(0));
      rc = strThis.compareTo(strOther);
    }

    return rc;
  }

}
