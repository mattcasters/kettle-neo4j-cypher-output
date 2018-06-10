package com.neo4j.kettle.steps.graph_output;

import com.neo4j.model.NeoValueMeta;
import com.neo4j.model.NeoValueType;
import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.util.ArrayList;
import java.util.List;

public class CypherOutputDialog extends BaseStepDialog implements StepDialogInterface {

  private static Class<?> PKG = CypherOutputMeta.class; // for i18n purposes, needed by Translator2!!

  private Label wlStepname;
  private Text wStepname;

  private Label wlConnection;
  private CCombo wConnection;

  private Label wlBatchSize;
  private TextVar wBatchSize;

  private Label wlCypher;
  private TextVar wCypher;

  private Label wlParameters;
  private TableView wParameters;

  private Label wlReturns;
  private TableView wReturns;

  private static String[] fieldNames;

  private CypherOutputMeta input;

  public CypherOutputDialog( Shell parent, Object inputMetadata, TransMeta transMeta, String stepname ) {
    super( parent, (BaseStepMeta)inputMetadata, transMeta, stepname );
    input = (CypherOutputMeta) inputMetadata;
  }

  @Override public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        input.setChanged();
      }
    };
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( "Neo4j Cypher Output" );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;


    // Step name line
    //
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( "Step name" );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.right = new FormAttachment( middle, -margin );
    fdlStepname.top = new FormAttachment( 0, margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, 0 );
    fdStepname.top = new FormAttachment( wlStepname, 0, SWT.CENTER);
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );
    Control lastControl = wStepname;

    wlConnection = new Label( shell, SWT.RIGHT );
    wlConnection.setText( "Connection" );
    props.setLook( wlConnection );
    FormData fdlConnection = new FormData();
    fdlConnection.left = new FormAttachment( 0, 0 );
    fdlConnection.right = new FormAttachment( middle, -margin );
    fdlConnection.top = new FormAttachment( lastControl, 2*margin );
    wlConnection.setLayoutData( fdlConnection );
    wConnection = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wConnection );
    wConnection.addModifyListener( lsMod );
    FormData fdConnection = new FormData();
    fdConnection.left = new FormAttachment( middle, 0 );
    fdConnection.right = new FormAttachment( 100, 0 );
    fdConnection.top = new FormAttachment( wlConnection, 0, SWT.CENTER );
    wConnection.setLayoutData( fdConnection );
    lastControl = wConnection;

    wlBatchSize = new Label( shell, SWT.RIGHT );
    wlBatchSize.setText( "Batch size (rows)" );
    props.setLook( wlBatchSize );
    FormData fdlBatchSize = new FormData();
    fdlBatchSize.left = new FormAttachment( 0, 0 );
    fdlBatchSize.right = new FormAttachment( middle, -margin );
    fdlBatchSize.top = new FormAttachment( lastControl, 2*margin );
    wlBatchSize.setLayoutData( fdlBatchSize );
    wBatchSize = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wBatchSize );
    wBatchSize.addModifyListener( lsMod );
    FormData fdBatchSize = new FormData();
    fdBatchSize.left = new FormAttachment( middle, 0 );
    fdBatchSize.right = new FormAttachment( 100, 0 );
    fdBatchSize.top = new FormAttachment( wlBatchSize, 0, SWT.CENTER );
    wBatchSize.setLayoutData( fdBatchSize );
    lastControl = wBatchSize;

    wlCypher = new Label( shell, SWT.LEFT);
    wlCypher.setText( "Cypher" );
    props.setLook( wlCypher );
    FormData fdlServers = new FormData();
    fdlServers.left = new FormAttachment( 0, 0 );
    fdlServers.right = new FormAttachment( middle, -margin );
    fdlServers.top = new FormAttachment( lastControl, margin );
    wlCypher.setLayoutData( fdlServers );
    wCypher = new TextVar( transMeta, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER );
    props.setLook( wCypher );
    wCypher.addModifyListener( lsMod );
    FormData fdServers = new FormData();
    fdServers.left = new FormAttachment( 0, 0 );
    fdServers.right = new FormAttachment( 100, 0 );
    fdServers.top = new FormAttachment( wlCypher, margin);
    fdServers.bottom = new FormAttachment( wlCypher, 300+margin );
    wCypher.setLayoutData( fdServers );
    lastControl = wCypher;

    // Some buttons
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    // Position the buttons at the bottom of the dialog.
    //
    setButtonPositions( new Button[] { wOK, wCancel }, margin, null );

    try {
      fieldNames = transMeta.getPrevStepFields( stepname ).getFieldNames();
    } catch(Exception e) {
      logError("Unable to get fields from previous steps", e);
      fieldNames = new String[] {};
    }
    
    // Table: parameter and field
    //
    ColumnInfo[] parameterColumns =
      new ColumnInfo[] {
        new ColumnInfo( "Parameter", ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( "Field", ColumnInfo.COLUMN_TYPE_CCOMBO, fieldNames, false ),
      };

    wlParameters = new Label( shell, SWT.LEFT);
    wlParameters.setText( "Parameters" );
    props.setLook( wlParameters );
    FormData fdlParameters = new FormData();
    fdlParameters.left = new FormAttachment( 0, 0 );
    fdlParameters.right = new FormAttachment( middle, -margin );
    fdlParameters.top = new FormAttachment( lastControl, margin );
    wlParameters.setLayoutData( fdlParameters );
    wParameters = new TableView( transMeta, shell, SWT.FULL_SELECTION | SWT.MULTI, parameterColumns, input.getParameterMappings().size(), lsMod, props );
    props.setLook( wParameters );
    wParameters.addModifyListener( lsMod );
    FormData fdParameters = new FormData();
    fdParameters.left = new FormAttachment( 0, 0 );
    fdParameters.right = new FormAttachment( 100, 0 );
    fdParameters.top = new FormAttachment( wlParameters, margin);
    fdParameters.bottom = new FormAttachment( wlParameters, 200+margin);
    wParameters.setLayoutData( fdParameters );
    lastControl = wParameters;

    // Table: return field name and type TODO Support more than String
    //
    ColumnInfo[] returnColumns =
      new ColumnInfo[] {
        new ColumnInfo( "Field name", ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( "Return type", ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "String" }, false ),
      };

    wlReturns = new Label( shell, SWT.LEFT);
    wlReturns.setText( "Returns" );
    props.setLook( wlReturns );
    FormData fdlReturns = new FormData();
    fdlReturns.left = new FormAttachment( 0, 0 );
    fdlReturns.right = new FormAttachment( middle, -margin );
    fdlReturns.top = new FormAttachment( lastControl, margin );
    wlReturns.setLayoutData( fdlReturns );
    wReturns = new TableView( transMeta, shell, SWT.FULL_SELECTION | SWT.MULTI, returnColumns, input.getReturnValues().size(), lsMod, props );
    props.setLook( wReturns );
    wReturns.addModifyListener( lsMod );
    FormData fdReturns = new FormData();
    fdReturns.left = new FormAttachment( 0, 0 );
    fdReturns.right = new FormAttachment( 100, 0 );
    fdReturns.top = new FormAttachment( wlReturns, margin);
    fdReturns.bottom = new FormAttachment( wlReturns, 200+margin);
    wReturns.setLayoutData( fdReturns );
    lastControl = wReturns;


    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wOK.addListener( SWT.Selection, lsOK );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wConnection.addSelectionListener( lsDef );
    wStepname.addSelectionListener( lsDef );
    wBatchSize.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;

  }

  private void cancel() {
    stepname = null;
    input.setChanged( changed );
    dispose();
  }

  public void getData() {

    wStepname.setText( Const.NVL( stepname, "" ) );
    wConnection.setText(Const.NVL(input.getConnectionName(), "") );
    wConnection.setItems( transMeta.getDatabaseNames() );
    wBatchSize.setText(Const.NVL(input.getBatchSize(), "") );
    wCypher.setText(Const.NVL(input.getCypher(), "") );

    for (int i=0;i<input.getParameterMappings().size();i++) {
      ParameterMapping mapping = input.getParameterMappings().get( i );
      TableItem item = wParameters.table.getItem( i );
      item.setText( 1, Const.NVL(mapping.getParameter(), ""));
      item.setText( 2, Const.NVL(mapping.getField(), ""));
    }
    wParameters.removeEmptyRows();
    wParameters.setRowNums();
    wParameters.optWidth( true );

    for (int i=0;i<input.getReturnValues().size();i++) {
      NeoValueMeta neoValueMeta = input.getReturnValues().get( i );
      TableItem item = wReturns.table.getItem( i );
      item.setText( 1, Const.NVL(neoValueMeta.getName(), ""));
      item.setText( 2, NeoValueType.getCode( neoValueMeta.getType() ));
    }
    wReturns.removeEmptyRows();
    wReturns.setRowNums();
    wReturns.optWidth( true );
  }

  private void ok() {
    if ( StringUtils.isEmpty( wStepname.getText() ) ) {
      return;
    }

    stepname = wStepname.getText(); // return value
    input.setConnectionName( wConnection.getText() );
    input.setBatchSize( wBatchSize.getText() );
    input.setCypher( wCypher.getText() );

    List<ParameterMapping> mappings = new ArrayList<ParameterMapping>(  );
    for (int i = 0;i<wParameters.nrNonEmpty();i++) {
      TableItem item = wParameters.getNonEmpty( i );
      mappings.add( new ParameterMapping( item.getText(1), item.getText(2)) );
    }
    input.setParameterMappings( mappings );

    List<NeoValueMeta> returnValues = new ArrayList<NeoValueMeta>();
    for (int i = 0;i<wReturns.nrNonEmpty();i++) {
      TableItem item = wReturns.getNonEmpty( i );
      returnValues.add( new NeoValueMeta( item.getText(1), NeoValueType.parseCode( item.getText(2))) );
    }
    input.setReturnValues( returnValues );

    dispose();
  }

}
