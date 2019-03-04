package org.locationtech.geowave.python;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * A class for debugging tools.
 */
public class Debug {

    /**
     * Prints information about the object on both python (returns a String) and java side.
     * @param obj
     */
    public String printObject(Object obj) {
        System.out.println(obj);
        return obj.toString();
    }

    /**
     * Prints (verbose) information about the object on both python (returns a String) and java side.
     * @param obj
     * @param verbose
     */
    public String printObject(Object obj, boolean verbose) {
        if (!verbose) {
            return printObject(obj);
        }

        String methods = "";

        for (Method method : obj.getClass().getMethods() ) {
            methods = methods + (method.getName() + " ;");
        }

        String fields = "";

        for (Field field : obj.getClass().getFields() ) {
            fields = fields + (field.getName() + "; ");
        }

        String info =   "Object: " + obj.toString() + "\n" +
                        "Class: " + obj.getClass().toString() + "\n" +
                        "isNull: " + (obj == null) + "\n" +
                        "Methods: " + methods + "\n" +
                        "Fields: " + fields + "\n";

        System.out.println(info);
        return info;
    }
}
