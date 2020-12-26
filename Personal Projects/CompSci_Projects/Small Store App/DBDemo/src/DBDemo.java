import java.sql.Connection;  
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.mysql.jdbc.PreparedStatement;

import tester.Tester;



/**
 * This class demonstrates how to connect to MySQL and run some basic commands.
 * 
 * In order to use this, you have to download the Connector/J driver and add
 * its .jar file to your build path.  You can find it here:
 * 
 * http://dev.mysql.com/downloads/connector/j/
 * 
 * You will see the following exception if it's not in your class path:
 * 
 * java.sql.SQLException: No suitable driver found for jdbc:mysql://localhost:3306/
 * 
 * To add it to your class path:
 * 1. Right click on your project
 * 2. Go to Build Path -> Add External Archives...
 * 3. Select the file mysql-connector-java-5.1.24-bin.jar
 *    NOTE: If you have a different version of the .jar file, the name may be
 *    a little different.
 *    
 * The user name and password are both "root", which should be correct if you followed
 * the advice in the MySQL tutorial. If you want to use different credentials, you can
 * change them below. 
 * 
 * You will get the following exception if the credentials are wrong:
 * 
 * java.sql.SQLException: Access denied for user 'userName'@'localhost' (using password: YES)
 * 
 * You will instead get the following exception if MySQL isn't installed, isn't
 * running, or if your serverName or portNumber are wrong:
 * 
 * java.net.ConnectException: Connection refused
 */

public class DBDemo {
  String DBuserName;
  String DBpassword;
  ArrayList<userPass> legalUserPasses;
  String currentUser;

  //constructor
  DBDemo(String DBuserName, String DBpassword) {
    this.DBuserName = DBuserName;
    this.DBpassword = DBpassword;
    try {
      this.legalUserPasses = this.fillUserPasses();
    } catch (SQLException e) {
      e.printStackTrace();
    }

    String currentUser = "No current Users!";
  }

  //populates the legalCharNames Array
  public ArrayList<userPass> fillUserPasses() throws SQLException {  
    // Connect to MySQL
    Connection conn = null;
    try {
      conn = this.getConnection();
    } catch (SQLException e) {
      System.out.println("ERROR: Could not connect to the database");
      e.printStackTrace();  
    }

    PreparedStatement p = (PreparedStatement) conn.prepareStatement("SELECT name FROM staff");
    ResultSet rs = p.executeQuery();

    List<String> result = new ArrayList<String>(); 
    while(rs.next()) {
      result.add(rs.getString(1));
    }

    PreparedStatement p1 = (PreparedStatement) conn.prepareStatement("SELECT password_string FROM staff s JOIN password p ON s.password_key = p.key");
    ResultSet rs1 = p1.executeQuery();

    ArrayList<String> result1 = new ArrayList<String>(); 
    while(rs1.next()) {
      result1.add(rs1.getString(1));
    }

    ArrayList<userPass> l = new ArrayList<userPass>();
    for(int i = 0; i < result.size(); i++) {
      userPass up = new userPass(result.get(i), result1.get(i));
      l.add(up);
    }

    System.out.println("legalCharNames list filled!");

    return l;
  }

  //Represents a Username/Password combo
  class userPass { 
    String username;
    String password;

    //Constructor
    userPass(String username, String password) {
      this.username = username;
      this.password = password;
    }

    //Checks if this userPass is legal
    boolean correctUserPass() {
      for(userPass up : legalUserPasses) {
        if(up.sameUserPass(this)) {
          return true;
        }
      }
      return false;
    }

    //Checks if this userPass and up are the same
    boolean sameUserPass(userPass up) {
      return username.equals(up.username) &&
          password.equals(up.password);
    }
  }



  /** The name of the computer running MySQL */
  private final String serverName = "localhost";

  /** The port of the MySQL server (default is 3306) */
  private final int portNumber = 3306;

  /** The name of the database we are testing with (this default is installed with MySQL) */
  private final String dbName = "mydb";


  /**
   * Get a new database connection
   * 
   * @return
   * @throws SQLException
   */
  public Connection getConnection() throws SQLException {
    Connection conn = null;
    Properties connectionProps = new Properties();
    connectionProps.put("user", this.DBuserName);
    connectionProps.put("password", this.DBpassword);

    conn = DriverManager.getConnection("jdbc:mysql://"
        + this.serverName + ":" + this.portNumber + "/" + this.dbName,
        connectionProps);

    return conn;
  }

  //Does the login operations:
  //   -Checks if Username/Password combo is correct
  private void login(Connection conn) {  
    Scanner scan = new Scanner(System.in);
    System.out.println("Please type your name: ");
    //scan.nextLine();
    String username = "David Mberingabo"; //TODO
    System.out.println("Please type your password: ");
    //scan.next();
    String password = "Password"; //TODO
    userPass up = new userPass(username, password);

    if(up.correctUserPass()) {
      System.out.println("Login Successful!");
      this.currentUser = username;

      System.out.println("Welcome " +this.currentUser+ "!");
      System.out.println("Would you like to buy something?");
      //scan.next();
      String ans = "yes"; //TODO

      if(ans.equals("yes")) {
        try {
          System.out.println("A theoretical shopping cart has been made for you!");
          new SaleReceipt().createShoppingCart(1, conn);
        } catch (SQLException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
      if(ans.equals("no")) {
        System.out.println("Then what are you doing using this application?!!");
      }
    }
    else {
      System.out.println("Login Failed!");
      System.out.println("Do you want to try logging in again?");
      String ans = scan.next();
      if(ans.equals("yes")) {
        this.login(conn);
      }
      if(ans.equals("no")) {
        System.out.println("Goodbye then!");
      }
    }



    System.out.println("");
  }








  //represents a collection of sale entries
  class SaleReceipt {
    ArrayList<Entry> saleEntries;

    //constructor
    SaleReceipt() {
      this.saleEntries = new ArrayList<Entry>();
    }

    //represents a sale Entry
    public class Entry { 
      int id;
      String goodName;
      int numPurchased;

      Entry(){}

      //constructor
      Entry(String goodName, int numPurchased, int id) {
        this.goodName = goodName;
        this.numPurchased = numPurchased;
        this.id = id;
      }


      public void addEntry(int id, Connection conn) throws SQLException { 

        //Asks for input and assigns it to an Entry
        Scanner scan = new Scanner(System.in);
        System.out.println("Please enter the good you would like to purchase");
        String goodName = scan.nextLine();
        System.out.println("Please enter the amount you would like to purchase");
        int numPurchased = scan.nextInt();

        Entry e = new Entry(goodName, numPurchased, id);

        if(e.legalEntry(conn) == 0) {
          System.out.println("Thank you! "+e.numPurchased+ " " +e.goodName+ " has been added to your shopping cart!");
          saleEntries.add(e);
        }
        else {
          System.out.println("");
          System.out.println("This is an illegal entry! It was not added to your cart!");
          System.out.println("Would you like to make a new entry?");
          String ans = scan.next();
          if(ans.equals("yes")) {
            this.addEntry(id, conn);
          }
          else if(ans.equals("no")){
           
          }
        }
      }

      //Is this entry legal?
      int legalEntry(Connection conn) throws SQLException {
        boolean a = false;
        boolean b = false;
        int n = 0;

        PreparedStatement p = (PreparedStatement) conn.prepareStatement("SELECT name FROM asset a JOIN good g ON g.asset_assetID = a.assetID");
        ResultSet rs = p.executeQuery();

        List<String> l = new ArrayList<String>(); 
        while(rs.next()) {
          l.add(rs.getString(1));
        }

        PreparedStatement p1 = (PreparedStatement) conn.prepareStatement("SELECT shelved FROM good g JOIN asset a ON g.asset_assetID = a.assetID WHERE a.name = \'"+this.goodName+"'");
        ResultSet rs1 = p1.executeQuery();


        while(rs1.next())
        {
          n = rs1.getInt(1);
          b = this.numPurchased <= n;
        }

        for(int i = 0; i < l.size(); i++) {
          if(this.goodName.equals(l.get(i))) {
            a = true;
          }
        }


        if(a && b) {
          System.out.println("It's a legal Entry!");
          return 0;
        }
        else if(a && !b) {
          System.out.println("Sorry, we only have " +n+ " of " + this.goodName + " in stock.");
          System.out.println("");
          return 1;
        }
        else if(!a && b) {
          System.out.println("we do not have any" +this.goodName+ " in stock");
          return 2;
        }
        else {
          return 3;
        }
      }

      //resturns this Entry's id
      public int getID() {
        return id;
      }
    }

    //Prompts user to create a shopping cart
    //Add Entries to the shopping cart
    private void createShoppingCart(int id, Connection conn) throws SQLException {
      Scanner scan = new Scanner(System.in);
      System.out.println("Would you like to add an entry to your shopping cart?");
      String ans = scan.next();
      if(ans.equals("yes")) {
        new Entry().addEntry(id, conn);
        this.createShoppingCart(id++, conn);
      }
      if(ans.equals("no")) {
        System.out.println("Your shopping cart is ready to be finalized!");
        this.printReceipt(saleEntries, conn);
      }
    }
    
    //Executes an Update to the DB by taking a SQL command
    public boolean executeUpdate(Connection conn, String command) throws SQLException {
      Statement stmt = null;
      try {
        stmt = conn.createStatement();
        stmt.executeUpdate(command); // This will throw a SQLException if it fails
        return true;
      } finally {

        // This will run whether we throw an exception or not
        if (stmt != null) { stmt.close(); }
      }
    }
    
    //Prints this receipt and updates the DB 
    private void printReceipt(ArrayList<Entry> l, Connection conn) throws SQLException {
      Scanner scan = new Scanner(System.in);
      int unit_price = 0;
      int total = 0;
      System.out.println("Receipt Begins: ");
      for(Entry e : l) {
        PreparedStatement p = (PreparedStatement) conn.prepareStatement("SELECT unit_price$ FROM asset a WHERE a.name = \'"+e.goodName+"'");
        ResultSet rs = p.executeQuery();

        while(rs.next()) {
          unit_price = rs.getInt(1);
        }
        
        System.out.println(e.id+ " " + e.goodName + "...." );
        System.out.println("   number purchased: " +e.numPurchased);
        System.out.println("   unit price: $" +(e.numPurchased*unit_price));
        
      }
      
      System.out.println("Total: " + this.receiptTotal(conn));

      System.out.println("Receipt End");
      System.out.println("");
      System.out.println("Type pay to pay and finalize your purchase");
      
      
      String ans = scan.next();
      
      System.out.println("");
      
      //UPDATING THE DATABASE
      //What doesn't work:
      //    -We could not record saleReceipts, because we could not figure out how to give each saleReceipts its own unique id and saleReceipts won't accept date_time to be its primary key.
      //    -We fix this for now by allowing groups of salesEntries made together to have their own time stamps.
      if(ans.equals("pay")) {
        for(Entry e : this.saleEntries) {
          
          //shows the OLD-shelved value
          PreparedStatement p = (PreparedStatement) conn.prepareStatement("SELECT shelved FROM good g JOIN asset a ON g.asset_assetID = a.assetID WHERE a.name = \""+e.goodName+"\"");
          ResultSet rs = p.executeQuery();
          int shelved = 0;

          while(rs.next()) {
           shelved = rs.getInt(1);
           System.out.println("Old-shelved: "+shelved);
          }
          int n = shelved - e.numPurchased;
          
          //Decrease each good's shelved attribute by numPurchased --WORKS
          this.executeUpdate(conn, "UPDATE  good g JOIN asset a ON g.asset_assetID = a.assetID SET shelved = "+n+" WHERE name = '" +e.goodName+"'");
          
          //shows the NEW-shelved value
          PreparedStatement p1 = (PreparedStatement) conn.prepareStatement("SELECT shelved FROM good g JOIN asset a ON g.asset_assetID = a.assetID WHERE a.name = \""+e.goodName+"\"");
          ResultSet rs1 = p1.executeQuery();
          int shelved1 = 0;

          while(rs1.next()) {
           shelved1 = rs1.getInt(1);
           System.out.println("New-shelved: "+shelved1);
          }
          
          DateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
          Date today = Calendar.getInstance().getTime();        
          String reportDate = df.format(today);

          // Print what date is today!
          System.out.println("Receipt DateTime Recorded: " + reportDate);
                    
          //Insert saleEntries 
          this.executeUpdate(conn, "INSERT INTO saleEntry (numSold, datetime) VALUES ("+e.numPurchased+", \""+reportDate+"\")");
          
          System.out.println("New saleEntry for "+e.numPurchased+" "+e.goodName+" has been inserted!");
          
          System.out.println("");
          System.out.println("DB Updated");
          System.out.println("Thanks for using the Application!");
        }
      }
            
    }
    
    //Calculates the net total of all the entries in this salesReceipt
    public int receiptTotal(Connection conn) throws SQLException {
      int n = 0;
      for(Entry e : this.saleEntries) {
        
        PreparedStatement p = (PreparedStatement) conn.prepareStatement("SELECT unit_price$ FROM asset a WHERE a.name = \'"+e.goodName+"'");
        ResultSet rs = p.executeQuery();
        
        int x = 0;

        while(rs.next()) {
         x = rs.getInt(1)*e.numPurchased;
        }
        
        n = n + x;
      }
      return n;
    }

  }

  public void run() throws SQLException {
    Scanner scan = new Scanner(System.in);

    // Connect to MySQL
    Connection conn = null;
    try {
      conn = this.getConnection();
      System.out.println("Connected to database!");
      System.out.println("");
    } catch (SQLException e) {
      System.out.println("ERROR: Could not connect to the database");
      e.printStackTrace();
      return;
    }

    System.out.println("Welcome to the Self Checkout Application, ");
    System.out.println("would you like to login?");
    String s = scan.next();

    if(s.equals("yes")) {
      this.login(conn);
    }
    else {
      System.out.println("Why are you using this application then??");
    }
  }


  public static void main(String[] args) throws SQLException {
    DBDemo app = new DBDemo("root", "Pasword1997");
    app.run();
  }
}


