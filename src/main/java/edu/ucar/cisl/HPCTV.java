package edu.ucar.cisl;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;

public class HPCTV {
    static String host = "sam-sql.ucar.edu";
    static String port = "3306";
    static String username = "username";
    static String password = "password";

    static String FILEPATH = "/path/to/sample/data";
    static String PROJCODE = "Project Code";
    static String COREHOURS = "Core Hours";
    static String MACHINE = "Machine";
    static String TITLE = "Project Title";
    static String AREAOFIINTEREST = "Area Of Interest";

    public static ArrayList<String> getMachinesForDate(Date date) {
        ArrayList<String> machines = new ArrayList();
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String connString = "jdbc:mysql://" + host + ":" + port + "/sam?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
            Connection con = DriverManager.getConnection(connString, username, password);

            PreparedStatement stmt = con.prepareStatement("select DISTINCT(machine) from sam.hpc_charge_summary where activity_date = ?");
            stmt.setDate(1, date);

            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                machines.add(rs.getString(1));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return machines;
    }

    public static ArrayList<ArrayList<String>> getDataForDate(String machine, Integer resultCount) {
        ArrayList<ArrayList<String>> dataForMachine = new ArrayList<ArrayList<String>>();
        try {
            Class.forName("com.mysql.jdbc.Driver");

            String connString = "jdbc:mysql://" + host + ":" + port + "/sam?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";

            Connection con = DriverManager.getConnection(connString, username, password);

            PreparedStatement stmt = con.prepareStatement("select hcs.projcode, sum(hcs.core_hours) as core_hours_sum , hcs.machine, " +
                    "p.title as 'project title', aoi.area_of_interest  from hpc_charge_summary  hcs\n" +
                    "inner join project p on hcs.projcode = p.projcode\n" +
                    "inner join area_of_interest aoi on p.area_of_interest_id = aoi.area_of_interest_id\n" +
                    "where activity_date = ? AND machine = ? group by hcs.projcode order by core_hours_sum desc limit ?;");

            java.util.Date date = new java.util.Date();
            String target = "2017-06-18";
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

            date = df.parse(target);

            java.sql.Date d = new java.sql.Date(date.getTime());
            stmt.setDate(1, d);
            stmt.setString(2, machine);
            stmt.setInt(3, resultCount);

            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
//                System.out.printf("%s, %s, %s, %s, %s\n", rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5));
                ArrayList<String> datum = new ArrayList<String>();
                datum.add(rs.getString(1));
                datum.add(rs.getString(2));
                datum.add(rs.getString(3));
                datum.add(rs.getString(4));
                datum.add(rs.getString(5));
                dataForMachine.add(datum);
            }

            con.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dataForMachine;
    }

    public static void writeCSVFile(ArrayList<ArrayList<String>> data, java.util.Date date, String machine) {

        DateFormat df = new SimpleDateFormat("MM-dd-yyyy");
        String fileName = machine + "-" + df.format(date) + ".csv";

        CSVFormat format = CSVFormat.EXCEL.withHeader(PROJCODE, COREHOURS, MACHINE, TITLE, AREAOFIINTEREST);
        try {
            FileWriter writer = new FileWriter(FILEPATH + "/" + fileName);
            CSVPrinter printer = new CSVPrinter(writer, format);
            printer.printRecords(data);
            printer.flush();
            printer.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("usage: startdate enddate resultscount");
            System.exit(1);
        }
        String startdateString = args[0];
        String enddateString = args[1];
        Integer resultCount = Integer.parseInt(args[2]);
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        try {
            java.util.Date startdate;
            java.util.Date enddate;
            startdate = df.parse(startdateString);
            enddate = df.parse(enddateString);
            while (startdate.before(enddate)) {
                java.sql.Date sqlDate = new java.sql.Date(startdate.getTime());
                System.out.println(startdate.toString());
                ArrayList<String> machines = getMachinesForDate(sqlDate);
                for (String m : machines) {
                    ArrayList<ArrayList<String>> dataForMachine = getDataForDate(m, resultCount);
                    writeCSVFile(dataForMachine, startdate, m);
                }
                Calendar c = Calendar.getInstance();
                c.setTime(startdate);
                c.add(Calendar.DATE, 1);
                startdate = c.getTime();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
