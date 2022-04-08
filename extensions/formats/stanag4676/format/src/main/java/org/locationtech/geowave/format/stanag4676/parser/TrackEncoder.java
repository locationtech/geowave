/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.stanag4676.parser;

import java.io.OutputStream;
import org.locationtech.geowave.format.stanag4676.parser.model.TrackRun;

public interface TrackEncoder {
  public void setOutputStreams(OutputStream trackOut, OutputStream missionOut);

  public void Encode(TrackRun run);
}
