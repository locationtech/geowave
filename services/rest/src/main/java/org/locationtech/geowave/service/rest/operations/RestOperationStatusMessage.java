/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest.operations;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class RestOperationStatusMessage {
  public enum StatusType {
    NONE, STARTED, RUNNING, COMPLETE, ERROR
  };

  /** A REST Operation message that is returned via JSON */
  @SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
  public StatusType status;

  @SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
  public String message;

  @SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
  public Object data;

  public RestOperationStatusMessage() {
    status = StatusType.NONE;
    message = "";
    data = null;
  }
}
