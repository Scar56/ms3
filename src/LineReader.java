import org.sqlite.SQLiteConnection;
import org.sqlite.javax.SQLiteConnectionPoolDataSource;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

public class LineReader extends Thread
{
    private String line;
    private String fileName;
    private String tableName;

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
        for (int i = 0; i < 10; i++)
        {
            if (i == 4 && lineTmp.contains("\""))
            {
                values[i] = lineTmp.substring(0, lineTmp.indexOf("\",") + 1);
                lineTmp = lineTmp.substring(lineTmp.indexOf("\",") + 2);

            } else
            {
                values[i] = lineTmp.substring(0, lineTmp.indexOf(','));
                lineTmp = lineTmp.substring(lineTmp.indexOf(',') + 1);

            }
            if (values[i].isEmpty())
            {
                valid = false;
                break;
            }
        }
        lineTmp = lineTmp.replaceAll(",", "");
        if (!lineTmp.isEmpty())
            valid = false;

        if (valid)
            insertSQL(values);
        else
            insertBad(line);

    }

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

    private final Lock _mutex = new ReentrantLock(true);

    private void insertBad(String line)
    {
        synchronized (_mutex)
        {
            try
            {
                BufferedWriter bw = new BufferedWriter(new FileWriter(fileName + "-bad.csv", true));
                bw.write(line + "\n");
                bw.flush();
            } catch (IOException e)
            {
                e.printStackTrace();
            }
        }


    }
}
