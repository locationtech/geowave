/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
/** */
package org.locationtech.geowave.core.cli.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import com.beust.jcommander.ParameterException;

/** Common file utilities, for performing common operations */
public class FileUtils {

  /**
   * Method to format file paths, similar to how command-line substitutions will function. For
   * example, we want to substitute '~' for a user's home directory, or environment variables
   *
   * @param filePath the file path to format
   * @return the formatted file path
   */
  public static String formatFilePath(String filePath) {
    if (filePath != null) {
      if (filePath.indexOf("~") != -1) {
        filePath = filePath.replace("~", System.getProperty("user.home", "~"));
      }
      if (filePath.indexOf("$") != -1) {
        int startIndex = 0;
        while ((startIndex != -1) && (filePath.indexOf("$", startIndex) != -1)) {
          final String variable = getVariable(filePath.substring(startIndex));
          final String resolvedValue = resolveVariableValue(variable);
          // if variable was not resolved to a system property, no
          // need to perform string replace
          if (!variable.equals(resolvedValue)) {
            filePath = filePath.replace(variable, resolvedValue);
          }
          startIndex = filePath.indexOf("$", (startIndex + 1));
        }
      }
    }
    return filePath;
  }

  /**
   * If an environment variable, or something resembling one, is detected - i.e. starting with '$',
   * try to resolve it's actual value for resolving a path
   *
   * @param variable the string to check
   * @return the variable name
   */
  private static String getVariable(final String variable) {
    final StringBuilder sb = new StringBuilder();
    char nextChar;
    for (int index = 0; index < variable.length(); index++) {
      nextChar = variable.charAt(index);
      if ((nextChar == '$')
          || Character.isLetterOrDigit(nextChar)
          || (nextChar != File.separatorChar)) {
        sb.append(nextChar);
      } else {
        break;
      }
    }
    return sb.toString();
  }

  private static String resolveVariableValue(final String variable) {
    if (System.getenv().containsKey(variable)) {
      return System.getenv(variable);
    } else if (System.getProperties().containsKey(variable)) {
      return System.getProperty(variable);
    }
    return variable;
  }

  /**
   * Reads the content of a file.
   *
   * @param inputFile the file to read
   * @return the contents of the file
   */
  public static String readFileContent(final File inputFile) throws Exception {
    Scanner scanner = null;
    try {
      scanner = new Scanner(inputFile, "UTF-8");
      return scanner.nextLine();
    } catch (final FileNotFoundException e) {
      throw new ParameterException(e);
    } finally {
      if (scanner != null) {
        scanner.close();
      }
    }
  }
}
