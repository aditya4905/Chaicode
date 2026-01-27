package Chaicode;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class EmployeeDB2App {
    // ---------------- DB CONNECTION ----------------
    static final String DB_URL =
            "jdbc:db2://db2i-amitparmar-ypm0c-x86.fyre.ibm.com:50000/testdb1";
    static final String USER = "testuser1";
    static final String PASS = "password123";
    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(DB_URL, USER, PASS);
    }
    // --------------------------------------------------
    // CREATE TABLE
    // --------------------------------------------------
    public static void createEmployeeTable() {
        String sql = """
            CREATE TABLE EMPLOYEE (
                EMP_ID INTEGER GENERATED ALWAYS AS IDENTITY
                    (START WITH 1, INCREMENT BY 1),
                EMP_NAME VARCHAR(100) NOT NULL,
                EMAIL VARCHAR(100) UNIQUE NOT NULL,
                DEPARTMENT VARCHAR(50),
                SALARY INTEGER,
                CREATED_AT TIMESTAMP DEFAULT CURRENT TIMESTAMP,
                PRIMARY KEY (EMP_ID)
            )
        """;
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            System.out.println(":white_check_mark: EMPLOYEE table created successfully");
        } catch (SQLException e) {
            if (e.getMessage().contains("SQL0601N")) {
                System.out.println(":warning: EMPLOYEE table already exists");
            } else {
                System.out.println(":x: Error creating table");
                e.printStackTrace();
            }
        }
    }
    // --------------------------------------------------
    // INSERT
    // --------------------------------------------------
    public static void addEmployee(String name, String email,
                                  String dept, int salary) {
        String sql = """
            INSERT INTO EMPLOYEE
            (EMP_NAME, EMAIL, DEPARTMENT, SALARY)
            VALUES (?, ?, ?, ?)
        """;
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, name);
            ps.setString(2, email);
            ps.setString(3, dept);
            ps.setInt(4, salary);
            ps.executeUpdate();
            System.out.println(":white_check_mark: Employee added");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    // --------------------------------------------------
    // READ
    // --------------------------------------------------
    public static void viewEmployees() {
        String sql = "SELECT * FROM EMPLOYEE";
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            System.out.println("\n:clipboard: EMPLOYEE TABLE:");
            while (rs.next()) {
                System.out.println(
                        "EMP_ID=" + rs.getInt("EMP_ID") +
                        ", NAME=" + rs.getString("EMP_NAME") +
                        ", EMAIL=" + rs.getString("EMAIL") +
                        ", DEPT=" + rs.getString("DEPARTMENT") +
                        ", SALARY=" + rs.getInt("SALARY") +
                        ", CREATED_AT=" + rs.getTimestamp("CREATED_AT")
                );
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    // --------------------------------------------------
    // UPDATE
    // --------------------------------------------------
    public static void updateSalary(int empId, int newSalary) {
        String sql = "UPDATE EMPLOYEE SET SALARY = ? WHERE EMP_ID = ?";
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, newSalary);
            ps.setInt(2, empId);
            ps.executeUpdate();
            System.out.println(":white_check_mark: Salary updated");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    // --------------------------------------------------
    // DELETE
    // --------------------------------------------------
    public static void deleteEmployee(int empId) {
        String sql = "DELETE FROM EMPLOYEE WHERE EMP_ID = ?";
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, empId);
            ps.executeUpdate();
            System.out.println(":white_check_mark: Employee deleted");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    // --------------------------------------------------
    // MAIN
    // --------------------------------------------------
    public static void main(String[] args) {
        // createEmployeeTable();
        addEmployee("Beast Brock lesner", "lulumall@gmail.com", "messi", 55000000);
        //addEmployee("Rahul", "rahul@gmail.com", "HR", 42000);
        viewEmployees();
        // updateSalary(1, 60000);
        // deleteEmployee(2);
        // viewEmployees();
        System.out.println("Hello, DB2!");
    }

}


