/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.cli.utils;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

/**
 * Performs a StringValue comparison of only the first element of equal-sized Lists of Objects, and
 * trivial sorting rules for lists.
 */
public class FirstElementListComparator implements Comparator<List<Object>>, Serializable {
  private static final long serialVersionUID = 1L;

  @Override
  public int compare(List<Object> listThis, List<Object> listOther) {
    // Re-factored to this awkward structure because of Spot Bugs
    if (listThis == null) {
      if (listOther == null) {
        return 0; // Consider both null as "equal"
      } else {
        return -1; // Null sorts ahead of non-null
      }
    } else if (listOther == null) {
      return 1; // Null sorts ahead of non-null
    }

    // At this point, neither list can be null
    if (listThis.size() != listOther.size()) {
      return listThis.size() - listOther.size(); // shorter list ahead of longer list
    } else { // lists are equal length
      if (listThis.size() > 0) {
        String strThis = String.valueOf(listThis.get(0) == null ? "" : listThis.get(0));
        String strOther = String.valueOf(listOther.get(0) == null ? "" : listOther.get(0));
        return strThis.compareTo(strOther);
      } else { // both lists are length zero
        return 0;
      }
    }
  }

}
