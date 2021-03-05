package osu.cse3241;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.Scanner;
import java.io.*;
import java.nio.file.Paths;


public class CSE3241Final {
	

    /**
     * The database file name.
     */
    private static String DATABASE = "CustomerServices.db";

    /**
     * The query statements to be executed.
     */
    private static String insertContract = "INSERT INTO CONTRACT (CLIENT_ID, Contract_number, Contract_Start_Date, Contract_value) VALUES (?, ?, ?, ?)";
    private static String insertClient = "INSERT INTO CLIENT (Client_ID, State, Company_name, Client_manager_name) VALUES (?, ?, ?, ?)";
    
    
    /**
     * Connects to the database if it exists, creates it if it does not, and
     * returns the connection object.
     *
     * @param databaseFileName
     *            the database file name
     * @return a connection object to the designated database
     */
    public static Connection initializeDB(String databaseFileName) {
        /**
         * The "Connection String" or "Connection URL".
         *
         * "jdbc:sqlite:" is the "subprotocol". (If this were a SQL Server
         * database it would be "jdbc:sqlserver:".)
         */
        String url = "jdbc:sqlite:" + databaseFileName;
        Connection conn = null; // If you create this variable inside the Try block it will be out of scope
        try {
            conn = DriverManager.getConnection(url);
            if (conn != null) {
                // Provides some positive assurance the connection and/or creation was successful.
                DatabaseMetaData meta = conn.getMetaData();
                System.out
                        .println("The driver name is " + meta.getDriverName());
                System.out.println(
                        "The connection to the database was successful.");
            } else {
                // Provides some feedback in case the connection failed but did not throw an exception.
                System.out.println("Null Connection");
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            System.out
                    .println("There was a problem connecting to the database.");
        }
        return conn;
    }
    /**
     * Gets input filename from user 
     *
     * @param reader
     *            a Scanner object to take input from the user
     * @returns the name of input file
     */
    public static String getInputFile(Scanner reader) {
    	String filename;
        System.out.println("Please enter the name of the input file (data/NewContracts.csv): ");
        filename = reader.nextLine(); // Get the User's choice
        return filename;
    }
    /**
     * Processes a single line from a csv file (updating client and contract information)
     *
     * @param data
     *            a String arry containing the csv line
     * @param stmt
     * 			  a PreparedStatement object
     * 
     * @param conn
     * 			  an object containing a connection to the database
     * 
     */
    public static void process(String[] data, PreparedStatement stmt, Connection conn) throws SQLException {
    	stmt = conn.prepareStatement(insertClient);
    	stmt.setInt(1, Integer.parseInt(data[0])); // Client ID
    	stmt.setString(2, data[1]); // State
    	stmt.setString(3, data[2]); // Company_name
    	stmt.setString(4,  data[3]); // Client_manager_name
    	stmt.executeUpdate();
    	
    	stmt = conn.prepareStatement(insertContract);
    	stmt.setInt(1, Integer.parseInt(data[0])); // Client ID
    	stmt.setInt(2, Integer.parseInt(data[4])); // Contract_Number
    	stmt.setString(3, data[5]); // Contract_start_date
    	stmt.setInt(4, Integer.parseInt(data[6])); // Contract value
    	stmt.executeUpdate(); // Insert Contract
    }
    
    public static void main(String[] args) throws SQLException {
        // Initialize the SQL variables
        Connection conn = null;
        PreparedStatement stmt = null;
        Scanner reader = new Scanner(System.in); // Allows the program to get input from the user
        String filename = getInputFile(reader); // Get the menu choice from the user
        try {
        	int linecount=1;
        	String line;
            conn = initializeDB(DATABASE);
            BufferedReader br = new BufferedReader(new FileReader(filename));
            line=br.readLine();
            while((line=br.readLine()) != null) {
            	System.out.println("Processing line " + linecount);
            	process(line.split(","), stmt, conn);
            	linecount += 1;
            }
            linecount -=1;
            System.out.println(linecount + " line(s) were processed.");
            if (br != null) {
            	br.close();
            }

        } catch (Exception ex) {
            System.out.println(ex);
        } finally {
            // Close all the open data streams (to prevent leaking)
            if (conn != null) {
                conn.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            if (reader != null) {
                reader.close();
            }
        }
    }
}
