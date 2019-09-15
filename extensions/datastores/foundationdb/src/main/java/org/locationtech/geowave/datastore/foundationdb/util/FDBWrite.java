package org.locationtech.geowave.datastore.foundationdb.util;

import com.apple.foundationdb.Transaction;

public class FDBWrite {
  private final byte[] key;
  private final byte[] value;

  public FDBWrite(byte[] key, byte[] value) {
    this.key = key;
    this.value = value;
  }

  public void add(Transaction txn) {
    txn.set(key, value);
  }
}
