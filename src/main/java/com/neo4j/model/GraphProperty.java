package com.neo4j.model;

public class NeoValueMeta {
  private String name;
  private NeoValueType type;

  public NeoValueMeta() {
  }

  public NeoValueMeta( String name, NeoValueType type ) {
    this.name = name;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public void setName( String name ) {
    this.name = name;
  }

  public NeoValueType getType() {
    return type;
  }

  public void setType( NeoValueType type ) {
    this.type = type;
  }
}
