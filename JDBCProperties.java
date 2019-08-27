package dbproperties;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;

public class JDBCProperties {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		/*try(OutputStream output=new FileOutputStream("app.properties")) {
			Properties prop=new Properties();
			prop.setProperty("db.type", "MySQL");
			prop.setProperty("db.driver", "mysql-connector-java-8.0.17.jar");
			prop.setProperty("db.url", "localhost");
			prop.setProperty("db.user", "root");
			prop.setProperty("db.password", "root");
			prop.store(output, "");
			System.out.println(prop);
		}
		catch(IOException io) {
			io.printStackTrace();
		}
		catch(Exception e) {
			e.printStackTrace();
		}*/
		Connection con;
		try(InputStream input=new FileInputStream("app.properties")) {
			if(input == null) {
				System.out.println("OOPS");
			}
			Properties prop=new Properties();
			prop.load(input);
			String dbt=prop.getProperty("db.type");
			String dbd=prop.getProperty("db.driver");
			String host=prop.getProperty("db.url");
			String user=prop.getProperty("db.user");
			String pass=prop.getProperty("db.password");
			
			System.out.println(prop);
			
			con=JDBCProperties.connect(dbt, dbd, host, user, pass);
			//JDBCProperties.testCon(con);
			String xmls=JDBCProperties.readData("employee", "*", "1", con);
			HashMap<String,String> hm=new HashMap<String,String>();
			hm.put("employee_id","11");
			hm.put("name","Alan");
			
			String res=JDBCProperties.saveData("employee", hm, con);
			
		}
		catch(IOException io) {
			io.printStackTrace();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
	}
	public static Connection connect(String db,String dbDriver,String host,String user,String pass) {
		Connection connection;
		try {
			Class.forName(dbDriver);
		} catch (ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			String url="jdbc:"+db+"://"+host+"/";
			connection = DriverManager
					.getConnection(url,user,pass);
			return connection;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public static void testCon(Connection con) {
		try {
			PreparedStatement stmt=con.prepareStatement("select * from `school`.courses");
			ResultSet rs=stmt.executeQuery();
			if(rs!=null) {
				System.out.println("Connection successful");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Connection failed");
		}
		
	}
	public static String readData(String tableName,String columnName,String value,Connection con) {
		String query="select "+columnName+" from `newschema`."+tableName+" where employee_id= "+value;
		StringBuilder xml=new StringBuilder();
		try {
			PreparedStatement stmt=con.prepareStatement(query);
			ResultSet rs=stmt.executeQuery();
			ResultSetMetaData rsmd=rs.getMetaData();
			if(rs!=null) {
				while(rs.next()) {
					
					xml.append("<Employee>");
					for(int i=1;i<=rsmd.getColumnCount();i++) {
						xml.append("<"+ rsmd.getColumnName(i) +">");
						xml.append(rs.getString(i));
						xml.append("</"+ rsmd.getColumnName(i) +">");
					}
					xml.append("</Employee>");
				}
			}
			return xml.toString();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public static String saveData(String tableName,HashMap<String,String>hm,Connection con) {
			String query="insert into `newschema`."+tableName+" (";

		try {
			
			for(int i=0;i<hm.entrySet().size();i++) {
				if(i==0) {
					query+=hm.keySet().toArray()[i];
				}
				else {
					query+=","+hm.keySet().toArray()[i];
				}
			}
			query+=") values(?,?);";
			PreparedStatement stmt=con.prepareStatement(query);
			int i=1;
			for(String s:hm.keySet()) {
				//stmt.setString(i, s);
				stmt.setString(i, hm.get(s));
				
				
				i++;
			}
			System.out.println(stmt);
			int a=stmt.executeUpdate();
			if(a>0 && a==hm.size()) {
				return "Update Success";
			}
			else if(a>0 && a<hm.size()) {
				return "Partial success";
			}
			else {
				return "Update failure";
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "Update failure";
		}
		
	}

}
