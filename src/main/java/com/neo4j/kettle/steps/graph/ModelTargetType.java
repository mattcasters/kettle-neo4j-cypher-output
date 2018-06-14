package com.neo4j.model;

import java.util.Arrays;

public enum GraphPropertyType {
  String,
  Integer,
  Float,
  Boolean,
  Date,
  LocalDateTime,
  ByteArray,
  Time,
  Point,
  Duration,
  LocalTime,
  DateTime
  ;

  /** Get the code for a type, handles the null case
   *
   * @param type
   * @return
   */

    public static String getCode(GraphPropertyType type) {
      if (type==null) {
        return null;
      }
      return type.name();
    }

  /**
   * Default to String in case we can't recognize the code or is null
   *
   * @param code
   * @return
   */
  public static GraphPropertyType parseCode( String code) {
      if (code==null) {
        return String;
      }
      try {
        return GraphPropertyType.valueOf( code );
      } catch(IllegalArgumentException e) {
        return String;
      }
    }

  public static String[] getNames() {
    String[] names = new String[values().length];
    for (int i=0;i<names.length;i++) {
      names[i] = values()[i].name();
    }
    return names;
  }
}