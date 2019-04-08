package org.locationtech.geowave.python;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * A class for debugging tools.
 */
public class Debug {

  /**
   * Prints information about the object on both python (returns a String) and java side.
   * 
   * @param obj
   */
  public String printObject(Object obj) {
    System.out.println(obj);
    return obj.toString();
  }

  /**
   * Prints (verbose) information about the object on both python (returns a String) and java side.
   * 
   * @param obj
   * @param verbose
   */
  public String printObject(Object obj, boolean verbose) {
    if (!verbose) {
      return printObject(obj);
    }

    StringBuilder methods = new StringBuilder();

    for (Method method : obj.getClass().getMethods()) {
      methods.append(method.getName()).append(" ;");
    }

    StringBuilder fields = new StringBuilder();

    for (Field field : obj.getClass().getFields()) {
      fields.append(field.getName()).append("; ");
    }
    StringBuilder info = new StringBuilder();
    info.append("Object: ").append(obj.toString()).append("\n").append("Class: ").append(
        obj.getClass().toString()).append("\n").append("isNull: ").append(obj == null).append(
            "\n").append("Methods: ").append(methods.toString()).append("\n").append(
                "Fields: ").append(fields.toString()).append("\n");

    System.out.println(info.toString());
    return info.toString();
  }
}
