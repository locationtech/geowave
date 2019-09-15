package org.locationtech.geowave.datastore.foundationdb.util;

import com.apple.foundationdb.Transaction;

public interface FDBInteraction {
  void add(Transaction txn);
}
