/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
/** */
package org.locationtech.geowave.core.cli.converters;

/**
 * Extends the password converter class to force required=false
 *
 * <p> This class will allow support for user's passing in passwords through a variety of ways.
 * Current supported options for passwords include standard password input (pass), an environment
 * variable (env), a file containing the password text (file), a properties file containing the
 * password associated with a specific key (propfile), and the user being prompted to enter the
 * password at command line (stdin). <br> <br> Required notation for specifying varying inputs are:
 *
 * <ul> <li><b>pass</b>:&lt;password&gt; <li><b>env</b>:&lt;variable containing the password&gt;
 * <li><b>file</b>:&lt;local file containing the password&gt; <li><b>propfile</b>:&lt;local
 * properties file containing the password&gt;<b>:</b>&lt;property file key&gt; <li><b>stdin</b>
 * </ul>
 */
public class OptionalPasswordConverter extends PasswordConverter {
  public OptionalPasswordConverter() {
    this("");
  }

  public OptionalPasswordConverter(final String optionName) {
    super(optionName);
  }

  @Override
  public String convert(final String value) {
    return super.convert(value);
  }

  @Override
  public boolean isPassword() {
    return true;
  }

  @Override
  public boolean isRequired() {
    return false;
  }
}
