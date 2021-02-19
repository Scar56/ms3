
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
        int successful = 0;
        int unsuccessful = 0;
        String tableName = "";
        String fileName = "";
        Connection connection = null;
        int numCPU = Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(numCPU);
        BufferedReader inputFile = null;
        BufferedReader br = null;
        BufferedWriter bw = null;

        if (args.length != 1)
        {
            System.out.println("Usage: java -jar ms3-1.0-jar-with-dependencies.jar {path to input file}");
        }
        try
        {
            File file = new File(args[0]);

            inputFile = new BufferedReader(new FileReader(args[0]));
            tableName = file.getName();
            tableName = tableName.substring(0, tableName.lastIndexOf('.'));
            fileName = args[0].substring(0, args[0].lastIndexOf('.'));
            String line;

            //delete old bad-records file if it exists
            try
            {
                File badRecords = new File(fileName + "-bad.csv");
                try
                {
                    badRecords.delete();
                } catch (Exception e)
                {
                    System.out.println("Write access denied");
                }
            } catch (Exception e)
            {
                //no file to delete, do nothing
            }
            try
            {
                // create a database connection
                connection = DriverManager.getConnection("jdbc:sqlite:" + fileName + ".db");
                Statement statement = connection.createStatement();
                statement.setQueryTimeout(30);  // set timeout to 30 sec.

                statement.executeUpdate("drop table if exists " + tableName);
                statement.executeUpdate("create table " + tableName + " (A string, B string, C string, D string, E string, F string, G string, H string, I string, J string)");

            }
            catch (SQLException e)
            {
                System.out.println("Cant create database, exiting");
                System.err.println(e.getMessage());
                return;
            }
            finally
            {
                try
                {
                    if (connection != null)
                        connection.close();
                }
                catch (SQLException e)
                {
                    // connection close failed.
                    System.err.println(e.getMessage());
                }
            }

            //skip the first line, its just column names
            inputFile.readLine();

            //parse each line in a new thread
            while ((line = inputFile.readLine()) != null)
            {
                Runnable lineReader = new LineReader(line, fileName, tableName);
                pool.execute(lineReader);
                recordsProcessed++;
            }

        }
        catch (IOException e)
        {
            System.out.println("Invalid file path");
        }
        finally
        {
            if (inputFile != null)
                inputFile.close();
            pool.shutdown();
            while (!pool.awaitTermination(2, TimeUnit.SECONDS)) ;
        }


        try
        {
            connection = DriverManager.getConnection("jdbc:sqlite:" + fileName + ".db");
            Statement statement = connection.createStatement();
            statement.setQueryTimeout(30);  // set timeout to 30 sec.

            ResultSet rs = statement.executeQuery("select Count(*) as count from " + tableName);

            successful = rs.getInt("count");
        }
        catch (SQLException e)
        {
            System.err.println(e.getMessage());
            return;
        }
        finally
        {
            try
            {
                if (connection != null)
                    connection.close();
            }
            catch (SQLException e)
            {
                // connection close failed.
                System.err.println(e.getMessage());
            }
        }

        try
        {
            br = new BufferedReader(new FileReader(fileName + "-bad.csv"));
            while (br.readLine() != null)
            {
                unsuccessful++;
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        finally
        {
            if(br != null)
                br.close();
        }

        try
        {
            bw = new BufferedWriter(
                    new FileWriter(fileName + ".log", true));
            bw.write("# of Records: " + recordsProcessed + System.lineSeparator());
            bw.write("# of Records Successful: " + successful + System.lineSeparator());
            bw.write("# of Records Failed: " + unsuccessful + System.lineSeparator());
            bw.flush();
            bw.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        finally
        {
            if(bw != null)
                bw.close();
        }
    }
}
