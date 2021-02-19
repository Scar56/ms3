import java.io.*;
import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CsvParser
{
    public static void main(String[] args) throws IOException, ClassNotFoundException
    {

        int numCPU = Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(numCPU);
        if (args.length != 1)
        {
            System.out.println("Usage: \"path to cvs file\"");
        }
        BufferedReader csvFile;
        try
        {
            File file = new File(args[0]);
            csvFile = new BufferedReader(new FileReader(args[0]));
            String filename = file.getName();
            filename = filename.substring(0, filename.lastIndexOf('.'));
            String line;
            Connection connection = null;
            try
            {
                // create a database connection
                connection = DriverManager.getConnection("jdbc:sqlite:/home/shaun/IdeaProjects/ms3/src/db.db");
                Statement statement = connection.createStatement();
                statement.setQueryTimeout(30);  // set timeout to 30 sec.

                statement.executeUpdate("drop table if exists " + filename);
                statement.executeUpdate("create table " + filename + " (A string, B string, C string, D string, E string, F string, G string, H integer, I integer, J string)");
//                statement.executeUpdate("insert into " + filename + " values(1, 'leo')");
//                statement.executeUpdate("insert into " + filename + " values(2, 'yui')");
//            ResultSet rs = statement.executeQuery("select * from person");
//            while(rs.next())
//            {
//                // read the result set
//                System.out.println("name = " + rs.getString("name"));
//                System.out.println("id = " + rs.getInt("id"));
//            }
            } catch (SQLException e)
            {
                System.err.println(e.getMessage());
                return;
            } finally
            {
                try
                {
                    if (connection != null)
                        connection.close();
                } catch (SQLException e)
                {
                    // connection close failed.
                    System.err.println(e.getMessage());
                }
            }

            //parse each line in a new thread
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
