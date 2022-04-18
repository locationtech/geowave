/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.plugin.transaction;

import java.io.IOException;

/**
 * Allocate a transaction ID. Controls the space of transaction IDs, allowing them to be reusable.
 * Essentially represents an unbounded pool of IDs. However, upper bound is determined by the number
 * of simultaneous transactions.
 *
 * <p> The set of IDs is associated with visibility/access.
 */
public interface TransactionsAllocator {
  public String getTransaction() throws IOException;

  public void releaseTransaction(String txID) throws IOException;
}
