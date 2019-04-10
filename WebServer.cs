using Data = System.Data;
using String = System.String;
using Object = System.Object;

/* WebServer which runs under IIS, passing requests to SQL server. Typical web.config file to pass all requests to WebServer is:

<?xml version="1.0" encoding="UTF-8"?>

<configuration>
  <system.web>
    <customErrors mode="Off"/>
  </system.web>

  <system.webServer>
    <handlers>
      <add name="WebServer" path="*" verb="*" type="WebServer" />
    </handlers>
  </system.webServer>
</configuration>

Typical SQL declaration of tables passed to SQL:

CREATE TYPE [perfect].[InfoT] AS TABLE( Kind int NOT NULL, Name nvarchar(100) NOT NULL, Value nvarchar(max) NOT NULL,
  PRIMARY KEY ( Name, Kind )
)

CREATE TYPE [perfect].[FileT] AS TABLE( id int NOT NULL, Name varchar(50) NOT NULL, ContentLength int NOT NULL,
  ContentType nvarchar(max) NULL, FileName nvarchar(200) NOT NULL, [File] image NULL, PRIMARY KEY ( id ) )
)

*/

public class WebServer : System.Web.IHttpHandler 
{

  private static Data.SqlClient.SqlConnection GetConn( ) 
  {
    return new Data.SqlClient.SqlConnection
    ( "Initial Catalog=redacted;Data Source=(Local);Max Pool Size=500;User Id=redcated;Password=redacted" );
  }

  public void ProcessRequest( System.Web.HttpContext ctx ) 
  {  
    // Each http request is handled by two SQL procedure calls. The first gets the name of the handling procedure, the second computes the response.

    try 
    { 
      using (  Data.SqlClient.SqlConnection sqlconn = GetConn() ) 
      using ( Data.SqlClient.SqlCommand cmd = new Data.SqlClient.SqlCommand( "perfect.GetHandler", sqlconn ) )
      {
        Data.SqlClient.SqlParameter p = null;

        { /* Set up table of info to be passed to stored procedure */
          Data.DataTable t = new Data.DataTable( );

          t.Columns.Add( "Kind", typeof( int ) );
          t.Columns.Add( "Name", typeof( string ) );
          t.Columns.Add( "Value", typeof( string ) );

          AddToDataTable( t, 0, ctx.Request.QueryString );
          AddToDataTable( t, 1, ctx.Request.Form );
          AddToDataTable( t, 2, ctx.Request.Cookies );

          t.Rows.Add( 3, "Host", ctx.Request.Url.Host );
          t.Rows.Add( 3, "Path", ctx.Request.Path );         
          t.Rows.Add( 3, "PathAndQuery", ctx.Request.Url.PathAndQuery );
          t.Rows.Add( 3, "IpAddress", ctx.Request.UserHostAddress );

          p = cmd.Parameters.AddWithValue( "@Info", t );
          p.SqlDbType = Data.SqlDbType.Structured; 
        }

        sqlconn.Open();
        cmd.CommandType = Data.CommandType.StoredProcedure;
        cmd.CommandText = ( string ) cmd.ExecuteScalar();

        if ( ctx.Request.Files.Count > 0 )
        {
          Data.DataTable ft = GetFileTable( ctx.Request.Files );
          p = cmd.Parameters.AddWithValue( "@Files", ft ); 
          p.SqlDbType = Data.SqlDbType.Structured;
        }

        Data.DataSet ds = new Data.DataSet( );
        using ( Data.SqlClient.SqlDataAdapter da = new Data.SqlClient.SqlDataAdapter( cmd ) )
        {
          da.Fill( ds );
        }

        // Interpret the dataset

        ctx.Response.ContentType = "text/html";
        String ShowRecordCount = null;
        for ( int i = 0; i < ds.Tables.Count; i += 1 )
        {
          Data.DataTable t = ds.Tables[i];
          if ( ShowRecordCount != null )
          {
            PutUtf8( ctx, ShowRecordCount + t.Rows.Count );
            ShowRecordCount = null;
          }
          for ( int j = 0; j < t.Rows.Count; j += 1 )
          { 
            Data.DataRow r = t.Rows[j];
            int code = 0; 
            Object value = r[0];
            if ( r.ItemArray.Length > 1 ) 
            {
              code = (int) value;
              value = r[1];
            }

            if ( code == 0 ) PutUtf8( ctx, (string) value );
            else if ( code == 1 ) ctx.Response.ContentType = (string) value; 
            else if ( code == 2 )
            {
              byte[] b = (byte[]) value;
              ctx.Response.OutputStream.Write( b, 0, b.Length );
            }
            else if ( code == 4 ) ctx.Response.Expires = (int) value;
            else if ( code == 14 ) ctx.Response.StatusCode = (int) value;
            else if ( code == 15 ) ctx.Response.Redirect( (string) value );
            else if ( code == 16 )
            {
              System.Web.HttpCookie ck = new System.Web.HttpCookie( (string) value, (string) r[2] );
              String Expires = (string) r[3];
              if ( Expires != "" ) ck.Expires = System.DateTime.Parse( Expires );
              ctx.Response.Cookies.Add( ck );
            }
            else if ( code == 17 ) ShowRecordCount = (string)value;
          }          
        }
      } 
    }
    catch ( System.Exception e )
    {
      ctx.Response.Write( e );
    }
  }

  public bool IsReusable { get { return true; } }
  
  private void AddToDataTable( Data.DataTable dt, int Kind, System.Collections.Specialized.NameValueCollection nvc )
  { 
    foreach ( string key in nvc.Keys ) 
      if ( key != null ) dt.Rows.Add( Kind, key, nvc[key] ); 
  }

  private void AddToDataTable(  Data.DataTable dt, int Kind, System.Web.HttpCookieCollection nvc )
  {
    foreach ( string key in nvc.Keys ) 
    {
      dt.Rows.Add( Kind, key, nvc[key].Value );
    }
  }

  private Data.DataTable GetFileTable( System.Web.HttpFileCollection fc )
  {
    Data.DataTable ft = new Data.DataTable();
    ft.Columns.Add( "id", typeof(int) );
    ft.Columns.Add( "Name", typeof(string) );
    ft.Columns.Add( "ContentLength", typeof(int) );
    ft.Columns.Add( "ContentType", typeof(string) );
    ft.Columns.Add( "FileName", typeof(string) );
    ft.Columns.Add( "File", typeof(byte[]) );
    for ( int id = 0; id < fc.Count; id += 1 )
    {
      System.Web.HttpPostedFile pf = fc[ id ];
      int length = pf.ContentLength;
      byte [] bytes = new byte[ length ];
      pf.InputStream.Read( bytes, 0, length );
      ft.Rows.Add( id, fc.GetKey(id), length, pf.ContentType, pf.FileName, bytes );
    }
    return ft;
  }

  // Output

  private byte [] EncBuffer = new byte[512];

  private static byte[] GetBuf( int need )
  { int n = 512; while ( n < need ) n *= 2; return new byte[n]; }

  private void PutUtf8( System.Web.HttpContext ctx, String s )
  {
    int len = s.Length;
    int need = System.Text.Encoding.UTF8.GetMaxByteCount( len );
    if ( need > EncBuffer.Length ) EncBuffer = GetBuf( need );
    int nb = System.Text.Encoding.UTF8.GetBytes( s, 0, len, EncBuffer, 0 );
    ctx.Response.OutputStream.Write( EncBuffer, 0, nb );
  }

  // Logging

  static WebServer()
  {
    System.AppDomain cd = System.AppDomain.CurrentDomain;
    cd.UnhandledException += new System.UnhandledExceptionEventHandler( LogException );
    Log( "Unhandled exception handler set" );
  }

  static void LogException( object sender, System.UnhandledExceptionEventArgs args ) 
  {
    System.Exception e = (System.Exception) args.ExceptionObject;
    Log( "Unhandled exception: " + e.ToString() );
  }

  static void Log( String message )
  {
    using ( Data.SqlClient.SqlConnection sqlconn = GetConn() )
    {
      using ( Data.SqlClient.SqlCommand cmd = new Data.SqlClient.SqlCommand( "perfect.Log", sqlconn ) )
      {
        cmd.CommandType = Data.CommandType.StoredProcedure;
        Data.SqlClient.SqlParameter p = cmd.Parameters.AddWithValue( "@Message", message );
        p.SqlDbType = Data.SqlDbType.NVarChar; 
        cmd.Connection.Open();
        cmd.ExecuteNonQuery();
      }
    }
  }

} // End class WebServer
