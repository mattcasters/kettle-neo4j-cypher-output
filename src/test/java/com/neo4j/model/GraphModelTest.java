package com.neo4j.model;

import com.neo4j.core.Neo4jDefaults;
import junit.framework.TestCase;
import org.junit.Ignore;
import org.junit.Test;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.stores.memory.MemoryMetaStore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GraphModelTest extends TestCase {

  private String TEST_NAME = "model-name";
  private String TEST_DESCRIPTION = "model-description";

  private String[][] TEST_NODES = new String[][] {
    { "Brand", "This is the beer brand", },
    { "Brewery", "The brewery that brews the beer", },
    { "BeerType", "The type of beer", },
    { "AlcoholPct", "The beer strength", }
  };

  private String[][] TEST_RELALTIONSHIPS = new String[][] {
    { "Brand-Brewery", "Brand", "Brewery", "BREWED_BY", "Describes the brand-brewery relationship", },
    { "Brand-BeerType", "Brand", "BeerType", "IS_OF_TYPE", "Describes the brand-beertype relationship", },
    { "Brand-%", "Brand", "AlcoholPct", "HAS_STRENGTH", "Describes the brand-strength relationship", }
  };


  private GraphModel model;

  @Override protected void setUp() throws Exception {
    model = createModel();
  }

  @Ignore
  private GraphModel createModel() {
    List<GraphNode> nodes = new ArrayList<>();
    for (String[] nodeStrings : TEST_NODES) {
      String name = nodeStrings[0];
      String description = nodeStrings[1];
      String label = name;

      List<String> labels = Arrays.asList( label );
      List<GraphProperty> properties = new ArrayList<>();
      GraphNode node = new GraphNode( nodeStrings[0], nodeStrings[1], labels, properties );
      nodes.add(node);
    }

    List<GraphRelationship> relationships = new ArrayList<>();
    for (String[] relationshipStrings : TEST_RELALTIONSHIPS) {
      String name = relationshipStrings[0];
      String from = relationshipStrings[1];
      String to = relationshipStrings[2];
      String type = relationshipStrings[3];
      String description = relationshipStrings[4];
      List<GraphProperty> properties = new ArrayList<>();

      GraphRelationship relationship = new GraphRelationship( name, description, type, properties, from, to );
      relationships.add(relationship);
    }

    GraphModel graphModel = new GraphModel( TEST_NAME, TEST_DESCRIPTION, nodes, relationships );
    return graphModel;
  }


  @Test
  public void getNameTest() {
    assertEquals( TEST_NAME, model.getName() );
  }

  @Test
  public void setName() {
    model.setName( "new"+ TEST_NAME );
    assertEquals( "new"+ TEST_NAME, model.getName() );
  }

  @Test
  public void getDescription() {
    assertEquals( TEST_DESCRIPTION, model.getDescription() );
  }

  @Test
  public void setDescription() {
    model.setName( "new"+ TEST_DESCRIPTION );
    assertEquals( "new"+ TEST_DESCRIPTION, model.getDescription() );
  }

  @Test
  public void getNodes() {
    assertEquals( TEST_NODES.length, model.getNodes().size() );
  }

  @Test
  public void setNodes() {
  }

  @Test
  public void getRelationships() {
    assertEquals( TEST_RELALTIONSHIPS.length, model.getRelationships().size() );
  }

  @Test
  public void setRelationships() {
  }

  @Test
  public void testGraphModelMetaStore() throws Exception {
    IMetaStore metaStore = new MemoryMetaStore();

    MetaStoreFactory<GraphModel> modelFactory = new MetaStoreFactory<>( GraphModel.class, metaStore, Neo4jDefaults.NAMESPACE );

    // Save the model
    //
    modelFactory.saveElement( model );

    // See if it's listed
    //
    List<String> modelNames = modelFactory.getElementNames();
    assertEquals( 1, modelNames.size() );
    assertEquals( TEST_NAME, modelNames.get( 0 ) );

    // Load it back in
    //
    GraphModel test = modelFactory.loadElement( TEST_NAME );

    compareModels(model, test);
  }

  @Test
  public void cloneTest() {
    GraphModel copy = model.clone();
    compareModels( model, copy );
  }

  @Ignore
  private void compareModels( GraphModel model, GraphModel test ) {
    assertEquals( model.getName(), test.getName() );
    assertEquals( model.getDescription(), test.getDescription() );

    // Check the nodes
    //
    assertEquals( model.getNodes().size(), test.getNodes().size() );
    for ( int i = 0; i < model.getNodes().size(); i++ ) {
      GraphNode modelNode = model.getNodes().get(i);
      GraphNode testNode = test.getNodes().get( i );

      assertEquals( modelNode.getName(), testNode.getName() );
      assertEquals( modelNode.getDescription(), testNode.getDescription() );

      assertEquals( modelNode.getLabels().size(), testNode.getLabels().size() );
      for (int j=0;j<modelNode.getLabels().size();j++) {
        assertEquals( modelNode.getLabels().get( j ), testNode.getLabels().get( j ) );
      }
    }

    // Check the relationships
    //
    assertEquals( model.getRelationships().size(), test.getRelationships().size() );
    for (int i=0;i<model.getRelationships().size();i++) {
      GraphRelationship modelRel = model.getRelationships().get(i);
      GraphRelationship testRel = test.getRelationships().get(i);

      assertEquals(modelRel.getName(), testRel.getName());
      assertEquals(modelRel.getDescription(), testRel.getDescription());
      assertEquals(modelRel.getNodeSource(), testRel.getNodeSource());
      assertEquals(modelRel.getNodeTarget(), testRel.getNodeTarget());
      assertEquals(modelRel.getLabel(), testRel.getLabel());
    }
  }
}