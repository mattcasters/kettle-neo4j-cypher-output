package com.neo4j.shared;

import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;

@RunWith( Arquillian.class )
public class NeoConnectionTest {
  @Deployment
  public static JavaArchive createDeployment() {
    return ShrinkWrap.create( JavaArchive.class )
      .addClass(. class)
  .addAsManifestResource( EmptyAsset.INSTANCE, "beans.xml" );
  }

  @org.junit.Test
  public void getUrl() {
  }
}
