import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LineReader extends Thread
{
    private final String line;
    private final String fileName;
    private final String tableName;
    private final Lock mutex = new ReentrantLock(true);

    /**
     * Asynchronously parse a row from the input file
     *
     * @param line      the line to parse
     * @param fileName  the full path of the input file without the extension
     * @param tableName the name of the table in the output db
     */
    public LineReader(String line, String fileName, String tableName)
    {
        this.line = line;
        this.fileName = fileName;
        this.tableName = tableName;
    }

    public void run()
    {
        boolean valid = true;
        String lineTmp = line;
        String[] values = new String[10];

        try{
        //extract items from row
        for (int i = 0; i < 10; i++)
        {
            //handle image in column E
            if (i == 4 && lineTmp.contains("\""))
            {
                values[i] = lineTmp.substring(0, lineTmp.indexOf("\",") + 1);
                lineTmp = lineTmp.substring(lineTmp.indexOf("\",") + 2);

            } else
            {
                values[i] = lineTmp.substring(0, lineTmp.indexOf(','));
                lineTmp = lineTmp.substring(lineTmp.indexOf(',') + 1);

            }

            //check that data exists
            if (values[i].isEmpty())
            {
                valid = false;
                break;
            }
        }}catch(Exception e){
            valid = false;
        }
        //check for extra columns
        if (valid)
        {
            lineTmp = lineTmp.replaceAll(",", "");
            if (!lineTmp.isEmpty())
                valid = false;
        }

        if (valid)
            insertSQL(values);
        else
            insertBad();

    }

    /**
     * insert the row into the db
     * @param values the parsed values from the 10 columns
     */
    private void insertSQL(String[] values)
    {
        Connection connection = null;
        while (true)
        {
            // create a database connection
            PreparedStatement ps = null;
            try
            {
                connection = DriverManager.getConnection("jdbc:sqlite:" + fileName + ".db");
                ps = connection.prepareStatement("insert into " + tableName + " values(?,?,?,?,?,?,?,?,?,?)");
                ps.setQueryTimeout(30);
                for (int i = 1; i < 10; i++)
                {
                    ps.setString(i + 1, values[i]);
                }
                ps.executeUpdate();
            } catch (SQLException e)
            {
                continue;
            } finally
            {
                try
                {
                    if (ps != null)
                        ps.close();
                    if (connection != null)
                        connection.close();
                } catch (SQLException e)
                {
                    // connection close failed.
                    System.err.println(e.getMessage());
                }
            }
            break;
        }
    }


    /**
     * Append the row to the bad.csv
     */
    private void insertBad()
    {
        try
        {
            mutex.lock();
            BufferedWriter bw = new BufferedWriter(
                    new FileWriter(fileName + "-bad.csv", true));
            bw.write(line + System.lineSeparator());
            bw.flush();
        } catch (IOException e)
        {
            e.printStackTrace();
        } finally
        {
            mutex.unlock();
        }
    }
}
