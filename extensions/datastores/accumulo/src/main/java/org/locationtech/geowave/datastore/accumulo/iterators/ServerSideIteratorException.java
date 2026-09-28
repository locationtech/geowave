/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo.iterators;

/**
 * A failure in GeoWave's own code inside a server-side iterator, such as a row that does not
 * decode. It is unchecked on purpose: a tablet server takes an IOException from a batch scan's
 * iterators for a failed read and has the client retry the lookup, which for a row that can never
 * be processed goes on forever. Any other exception fails the scan and reaches the client.
 */
class ServerSideIteratorException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  ServerSideIteratorException(final String message, final Exception cause) {
    super(message, cause);
  }
}
