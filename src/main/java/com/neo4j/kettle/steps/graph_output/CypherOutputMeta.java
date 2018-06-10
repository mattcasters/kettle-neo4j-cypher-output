package com.neo4j.kettle.steps.graph_output;

import com.neo4j.model.NeoValueMeta;
import com.neo4j.model.NeoValueType;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

@Step(
  id = "Neo4jCypherOutput",
  name = "Neo4j Cypher Output",
  description = "Write fields Neo4j using Cypher with parameters",
  image = "neo4j_logo.svg",
  categoryDescription = "Neo4j"
)
public class CypherOutputMeta extends BaseStepMeta implements StepMetaInterface {

  private String connectionName;
  private String cypher;
  private String batchSize;
  private List<ParameterMapping> parameterMappings;
  private List<NeoValueMeta> returnValues;

 public CypherOutputMeta() {
   super();
   parameterMappings = new ArrayList<ParameterMapping>();
   returnValues = new ArrayList<NeoValueMeta>();
  }

  @Override public void setDefault() {

  }

  @Override public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int i, TransMeta transMeta, Trans trans ) {
    return new CypherOutput( stepMeta, stepDataInterface, i, transMeta, trans );
  }

  @Override public StepDataInterface getStepData() {
    return new CypherOutputData();
  }

  @Override public String getDialogClassName() {
    return CypherOutputDialog.class.getName();
  }

  @Override public void getFields( RowMetaInterface rowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space,
                                   Repository repository, IMetaStore metaStore ) {

    // Check return values in the metadata...
    for (NeoValueMeta neoValueMeta : returnValues) {
      NeoValueType type = neoValueMeta.getType();
      if (type==null) {
        type = NeoValueType.String;
      }
      switch(type) {
        case String:
        default:
          rowMeta.addValueMeta( new ValueMetaString(neoValueMeta.getName()) );
      };
    }


   // No output fields for now
  }

  @Override public String getXML() {
    StringBuilder xml = new StringBuilder( );
    xml.append( XMLHandler.addTagValue( "connection", connectionName) );
    xml.append( XMLHandler.addTagValue( "cypher", cypher) );
    xml.append( XMLHandler.addTagValue( "batch_size", batchSize) );
    xml.append( XMLHandler.openTag( "mappings") );
    for (ParameterMapping parameterMapping : parameterMappings) {
      xml.append( XMLHandler.openTag( "mapping") );
      xml.append( XMLHandler.addTagValue( "parameter", parameterMapping.getParameter()) );
      xml.append( XMLHandler.addTagValue( "field", parameterMapping.getField() ) );
      xml.append( XMLHandler.closeTag( "mapping") );
    }
    xml.append( XMLHandler.closeTag( "mappings") );

    xml.append( XMLHandler.openTag( "returns") );
    for (NeoValueMeta returnValue : returnValues) {
      xml.append( XMLHandler.openTag( "return") );
      xml.append( XMLHandler.addTagValue( "name", returnValue.getName()) );
      xml.append( XMLHandler.addTagValue( "type", NeoValueType.getCode( returnValue.getType() ) ) );
      xml.append( XMLHandler.closeTag( "return") );
    }
    xml.append( XMLHandler.closeTag( "returns") );


    return xml.toString();
  }

  @Override public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    connectionName = XMLHandler.getTagValue( stepnode, "connection" );
    cypher = XMLHandler.getTagValue( stepnode, "cypher" );
    batchSize = XMLHandler.getTagValue( stepnode, "batch_size" );

    // Parse parameter mappings
    //
    Node mappingsNode = XMLHandler.getSubNode( stepnode, "mappings" );
    List<Node> mappingNodes = XMLHandler.getNodes( mappingsNode, "mapping" );
    parameterMappings = new ArrayList<ParameterMapping>();
    for (Node mappingNode : mappingNodes) {
      String parameter = XMLHandler.getTagValue( mappingNode, "parameter" );
      String field = XMLHandler.getTagValue( mappingNode, "field" );
      parameterMappings.add(new ParameterMapping( parameter, field ));
    }

    // Parse return values
    //
    Node returnsNode = XMLHandler.getSubNode( stepnode, "returns" );
    List<Node> returnNodes = XMLHandler.getNodes( returnsNode, "return" );
    returnValues = new ArrayList<NeoValueMeta>();
    for (Node returnNode : returnNodes) {
      String name = XMLHandler.getTagValue( returnNode, "name" );
      NeoValueType type = NeoValueType.parseCode( XMLHandler.getTagValue( returnNode, "type" ) );
      returnValues.add(new NeoValueMeta( name, type));
    }

    super.loadXML( stepnode, databases, metaStore );
  }

  @Override public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step ) throws KettleException {
    rep.saveStepAttribute( id_transformation, id_step, "connection", connectionName);
    rep.saveStepAttribute( id_transformation, id_step, "cypher", cypher);
    rep.saveStepAttribute( id_transformation, id_step, "batch_size", batchSize);
    for (int i=0;i<parameterMappings.size();i++) {
      ParameterMapping parameterMapping = parameterMappings.get( i );
      rep.saveStepAttribute( id_transformation, id_step, i, "parameter",  parameterMapping.getParameter());
      rep.saveStepAttribute( id_transformation, id_step, i, "field",  parameterMapping.getField() );
    }
    for (int i=0;i<returnValues.size();i++) {
      NeoValueMeta neoValueMeta = returnValues.get( i );
      rep.saveStepAttribute( id_transformation, id_step, i, "return_name",  neoValueMeta.getName());
      rep.saveStepAttribute( id_transformation, id_step, i, "return_type",  NeoValueType.getCode( neoValueMeta.getType() ) );
    }

  }

  @Override public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases ) throws KettleException {
    connectionName = rep.getStepAttributeString( id_step, "connection" );
    cypher = rep.getStepAttributeString( id_step, "cypher" );
    batchSize = rep.getStepAttributeString( id_step, "batch_size" );
    parameterMappings = new ArrayList<ParameterMapping>();
    int nrMappings = rep.countNrStepAttributes( id_step, "parameter" );
    for (int i=0;i<nrMappings;i++) {
      String parameter = rep.getStepAttributeString( id_step, i, "parameter" );
      String cypher = rep.getStepAttributeString( id_step, i, "field" );
      parameterMappings.add( new ParameterMapping( parameter, cypher) );
    }
    returnValues = new ArrayList<NeoValueMeta>();
    int nrReturns = rep.countNrStepAttributes( id_step, "return_name" );
    for (int i=0;i<nrReturns;i++) {
      String name = rep.getStepAttributeString( id_step, i, "return_name" );
      NeoValueType type = NeoValueType.parseCode( rep.getStepAttributeString( id_step, i, "return_type" ) );
      returnValues.add(new NeoValueMeta( name, type ));
    }

  }

  public String getConnectionName() {
    return connectionName;
  }

  public void setConnectionName( String connectionName ) {
    this.connectionName = connectionName;
  }

  public String getCypher() {
    return cypher;
  }

  public void setCypher( String cypher ) {
    this.cypher = cypher;
  }

  public List<ParameterMapping> getParameterMappings() {
    return parameterMappings;
  }

  public void setParameterMappings( List<ParameterMapping> parameterMappings ) {
    this.parameterMappings = parameterMappings;
  }

  public String getBatchSize() {
    return batchSize;
  }

  public void setBatchSize( String batchSize ) {
    this.batchSize = batchSize;
  }

  public List<NeoValueMeta> getReturnValues() {
    return returnValues;
  }

  public void setReturnValues( List<NeoValueMeta> returnValues ) {
    this.returnValues = returnValues;
  }
}
