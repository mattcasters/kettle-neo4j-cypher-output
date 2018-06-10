package com.neo4j.model;

public enum NeoValueType {
    Integer,
    Float,
    String,
    Boolean,
    Point,
    Date,
    Time,
    LocalTime,
    DateTime,
    LocalDateTime,
    Duration
  ;

  /** Get the code for a type, handles the null case
   *
   * @param type
   * @return
   */

    public static String getCode(NeoValueType type) {
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
  public static NeoValueType parseCode(String code) {
      if (code==null) {
        return String;
      }
      try {
        return NeoValueType.valueOf( code );
      } catch(IllegalArgumentException e) {
        return String;
      }
    }
}