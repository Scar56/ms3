import java.io.*;
import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CsvParser
{
    public static void main(String[] args) throws IOException, ClassNotFoundException
    {
        Connection connection = null;
        try
        {
            // create a database connection
            connection = DriverManager.getConnection("jdbc:sqlite:/home/shaun/IdeaProjects/ms3/src/db.db");
            Statement statement = connection.createStatement();
            statement.setQueryTimeout(30);  // set timeout to 30 sec.

            statement.executeUpdate("drop table if exists person");
            statement.executeUpdate("create table person (id integer, name string)");
            statement.executeUpdate("insert into person values(1, 'leo')");
            statement.executeUpdate("insert into person values(2, 'yui')");
            ResultSet rs = statement.executeQuery("select * from person");
            while(rs.next())
            {
                // read the result set
                System.out.println("name = " + rs.getString("name"));
                System.out.println("id = " + rs.getInt("id"));
            }
        }
        catch(SQLException e)
        {
            // if the error message is "out of memory",
            // it probably means no database file is found
            System.err.println(e.getMessage());
        }
        finally
        {
            try
            {
                if(connection != null)
                    connection.close();
            }
            catch(SQLException e)
            {
                // connection close failed.
                System.err.println(e.getMessage());
            }
        }
        //____________________________________________________________________________________________________________________________________________
        // creates five tasks
        // creates a thread pool with MAX_T no. of
        // threads as the fixed pool size(Step 2)
        int numCPU = Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(numCPU);

        // passes the Task objects to the pool to execute (Step 3)

        // pool shutdown ( Step 4)


        if (args.length != 1)
        {
            System.out.println("Usage: \"path to cvs file\"");
        }
        BufferedReader csvFile;
        try
        {
            csvFile = new BufferedReader(new FileReader(args[0]));
            String line;
            while ((line = csvFile.readLine()) != null)
            {
                Runnable lineReader = new LineReader(line);
                pool.execute(lineReader);
            }
            csvFile.close();

        } catch (IOException e)
        {
            System.out.println("Invalid file path");
        } finally
        {
            pool.shutdown();
        }
    }
}
