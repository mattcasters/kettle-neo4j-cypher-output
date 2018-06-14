package com.neo4j.kettle.steps.graph;

import com.neo4j.model.GraphPropertyType;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.ValueMetaInterface;

import java.time.ZoneId;

public class ParameterMapping {

  private String parameter;
  private String field;
  private String neoType;

  public ParameterMapping() {
  }

  public ParameterMapping( String parameter, String field, String neoType ) {
    this.parameter = parameter;
    this.field = field;
    this.neoType = neoType;
  }

  public String getParameter() {
    return parameter;
  }

  public void setParameter( String parameter ) {
    this.parameter = parameter;
  }

  public String getField() {
    return field;
  }

  public void setField( String field ) {
    this.field = field;
  }

  public String getNeoType() {
    return neoType;
  }

  public void setNeoType( String neoType ) {
    this.neoType = neoType;
  }

  /**
   * Convert the given Kettle value to a Neo4j data type
   * @param valueMeta
   * @param valueData
   * @return
   */
  public Object convertFromKettle( ValueMetaInterface valueMeta, Object valueData ) throws KettleValueException {
    GraphPropertyType type = GraphPropertyType.parseCode( neoType );
    if (valueMeta.isNull(valueData)) {
      return null;
    }
    switch(type) {
      case String: return valueMeta.getString( valueData );
      case Boolean: return valueMeta.getBoolean( valueData );
      case Float: return valueMeta.getNumber( valueData );
      case Integer: return valueMeta.getInteger( valueData );
      case Date: return valueMeta.getDate( valueData ).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
      case LocalDateTime: return valueMeta.getDate( valueData ).toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
      case ByteArray: return valueMeta.getBinary( valueData );
      case Duration:
      case DateTime:
      case Time:
      case Point:
      case LocalTime:
      default:
        throw new KettleValueException( "Data conversion to Neo4j type '"+neoType+"' of parameter '"+parameter+"' is not supported yet" );
    }
  }
}
