package com.neo4j.model;

import org.pentaho.metastore.persist.MetaStoreAttribute;

public class GraphNodeProperty {

  @MetaStoreAttribute
  private String name;

  @MetaStoreAttribute
  private String value;

  @MetaStoreAttribute
  private NeoValueType type;

  public GraphNodeProperty() {

  }

  public GraphNodeProperty( String name, String value, NeoValueType type ) {
    this();
    this.name = name;
    this.value = value;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public void setName( String name ) {
    this.name = name;
  }

  public String getValue() {
    return value;
  }

  public void setValue( String value ) {
    this.value = value;
  }

  public NeoValueType getType() {
    return type;
  }

  public void setType( NeoValueType type ) {
    this.type = type;
  }
}
