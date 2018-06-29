package com.neo4j.shared;

import junit.framework.TestCase;
import org.junit.Test;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;

import static org.junit.Assert.*;

public class NeoConnectionTest extends TestCase {

  @Test
  public void testGetUrlBolt() {
    VariableSpace space = new Variables();
    space.setVariable( "SERVER", "my-server" );

    NeoConnection nc = new NeoConnection(space);
    nc.setBoltPort( "7687" );
    nc.setServer("server");
    nc.setRouting( false );
    nc.setRoutingPolicy( "none" );

    assertEquals( "Bolt URL encoding failed", "bolt://server:7687", nc.getUrl());
    nc.setServer("${SERVER}");

    assertEquals( "Bolt URL encoding w/ variable", "bolt://my-server:7687", nc.getUrl());

  }

  @Test
  public void testGetUrlBoltRouting() {
    VariableSpace space = new Variables();
    space.setVariable( "SERVER", "my-server" );

    NeoConnection nc = new NeoConnection(space);
    nc.setBoltPort( "7687" );
    nc.setServer("server");
    nc.setRouting( true );
    nc.setRoutingPolicy( null );
    nc.setServer("server");

    assertEquals( "Bolt URL encoding w/ routing failed", "bolt+routing://server:7687", nc.getUrl());
    nc.setRoutingPolicy( "MyPolicy");
    assertEquals( "Bolt URL encoding w/ routing and policy failed", "bolt+routing://server:7687?policy=MyPolicy", nc.getUrl());

    nc.setRoutingPolicy( null );
    nc.setServer("${SERVER}");
    assertEquals( "Bolt URL encoding w/ routing and variable", "bolt+routing://my-server:7687", nc.getUrl());
    nc.setRoutingPolicy( "MyPolicy");
    assertEquals( "Bolt URL encoding w/ routing and policy failed", "bolt+routing://my-server:7687?policy=MyPolicy", nc.getUrl());

  }
}
