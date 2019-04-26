
import java.sql.*;

import java.util.Random;



import simpledb.remote.SimpleDriver;



public class CreateTestTable {

    final static int maxSize = 100;



    /**

     * @param args

     */

    public static void main(String[] args) {

        // TODO Auto-generated method stub

        Connection conn = null;

        Driver d = new SimpleDriver();

        String host = "localhost"; //you may change it if your SimpleDB server is running on a different machine

        String url = "jdbc:simpledb://" + host;

        String qry = "Create table test1" +

                "( a1 int," +

                "  a2 int" +

                ")";

        Random rand = null;

        Statement s = null;

        try {

            conn = d.connect(url, null);

            s = conn.createStatement();

            s.executeUpdate("Create table test1" +

                    "( a1 int," +

                    "  a2 int" +

                    ")");

            s.executeUpdate("Create table test2" +

                    "( a1 int," +

                    "  a2 int" +

                    ")");

            s.executeUpdate("Create table test3" +

                    "( a1 int," +

                    "  a2 int" +

                    ")");

            s.executeUpdate("Create table test4" +

                    "( a1 int," +

                    "  a2 int" +

                    ")");

            s.executeUpdate("Create table test5" +

                    "( a1 int," +

                    "  a2 int" +

                    ")");



            //test 4 table is the no index one

            s.executeUpdate("create sh index idx1 on test1 (a1)");

            s.executeUpdate("create eh index idx2 on test2 (a1)");

            s.executeUpdate("create bt index idx3 on test3 (a1)");

            for (int i = 1; i < 6; i++) {

                if (i != 5) {

                    rand = new Random(1);// ensure every table gets the same data

                    for (int j = 0; j < maxSize; j++) {

                        s.executeUpdate("insert into test" + i + " (a1,a2) values(" + rand.nextInt(1000) + "," + rand.nextInt(1000) + ")");

                        //System.out.println(j);

                    }

                } else//case where i=5

                {

                    for (int j = 0; j < maxSize / 2; j++)// insert 10000 records into test5

                    {

                        s.executeUpdate("insert into test" + i + " (a1,a2) values(" + j + "," + j + ")");

                        //System.out.println(j);

                    }

                }

            }



            for(int i=1;i<5;i++) {

                rand = new Random(1);

                long startTime = System.nanoTime();

                

                qry = "select a1,a2 from test" + i + " where a1=" + rand.nextInt(5);

                ResultSet rs = s.executeQuery(qry);

                rs.close();

                long endTime = System.nanoTime();

                double timeInMillis = (endTime - startTime)/1000000;

                System.out.println("Total time for querying test" + i + ": " + timeInMillis);

            }



            for(int i=1;i<5;i++) {

                rand = new Random(1);

                long startTime = System.nanoTime();

                qry = "select a1,a2 from test5,test" + i + " where a1=a1";

                ResultSet rs = s.executeQuery(qry);

                rs.close();

                long endTime = System.nanoTime();

                double timeInMillis = (endTime - startTime)/1000000;

                System.out.println("Total time for joining test5 with test" + i + ": " + timeInMillis);

            }

            conn.close();



        } catch (SQLException e) {

            // TODO Auto-generated catch block

            e.printStackTrace();

        } finally {

            try {

                conn.close();

            } catch (SQLException e) {

                // TODO Auto-generated catch block

                e.printStackTrace();

            }

        }

    }

}
