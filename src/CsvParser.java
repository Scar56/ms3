import java.io.*;
import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CsvParser
{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {

        int recordsProcessed = 0;
        String tablename = "";
        String fileName = "";
        Connection connection = null;
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
            tablename = file.getName();
            tablename = tablename.substring(0, tablename.lastIndexOf('.'));
            fileName = args[0].substring(0, args[0].lastIndexOf('.'));
            String line;
            try
            {
                // create a database connection
                connection = DriverManager.getConnection("jdbc:sqlite:" + fileName + ".db");
                Statement statement = connection.createStatement();
                statement.setQueryTimeout(30);  // set timeout to 30 sec.

                statement.executeUpdate("drop table if exists " + tablename);
                statement.executeUpdate("create table " + tablename + " (A string, B string, C string, D string, E string, F string, G string, H string, I string, J string)");

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
            line = csvFile.readLine();

            //parse each line in a new thread
            while ((line = csvFile.readLine()) != null)
            {
                Runnable lineReader = new LineReader(line, fileName, tablename);
                pool.execute(lineReader);
                recordsProcessed++;
            }
            csvFile.close();

        } catch (IOException e)
        {
            System.out.println("Invalid file path");
        } finally
        {
            pool.shutdown();
            while (!pool.awaitTermination(2, TimeUnit.SECONDS)) ;
        }
        int successful = 0;
        int unsuccessful = 0;


        try
        {
            // create a database connection
            connection = DriverManager.getConnection("jdbc:sqlite:" + fileName + ".db");
            Statement statement = connection.createStatement();
            statement.setQueryTimeout(30);  // set timeout to 30 sec.

            ResultSet rs = statement.executeQuery("select Count(*) as count from " + tablename);

            successful = rs.getInt("count");
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
        try
        {
            BufferedReader br = new BufferedReader(new FileReader(fileName + "-bad.csv"));
            while (br.readLine() != null)
            {
                unsuccessful++;
            }
            System.out.println("suc= " + successful);
            System.out.println("un= " + unsuccessful);
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
