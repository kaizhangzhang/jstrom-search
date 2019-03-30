package com.netease.kaola.distmerchant.common.util.db;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.sql.*;
import java.util.*;

/**
 * @author kai.zhang
 * @since 2019/2/2
 */
public class JdbcUtil {
    public static Connection getConnection(String driver, String url, String userName, String password) throws Exception {
        Class.forName(driver);
        Connection connection = DriverManager.getConnection(url, userName, password);
        return connection;
    }

    /**
     * 增 删 改
     * @return
     */
    public static boolean executeSql(Connection conn, String sql, List<Object> params) throws SQLException {
        boolean flag = false;
        if (StringUtils.isBlank(sql)) {
            return false;
        }
        PreparedStatement preparedStatement = conn.prepareStatement(sql);
        if (CollectionUtils.isNotEmpty(params)) {
            for (int i = 0; i < params.size(); i++) {
                preparedStatement.setObject(i + 1, params.get(i));
            }
        }
        int result = preparedStatement.executeUpdate();
        closeResource(preparedStatement);
        flag = result > 0 ? true : false;
        return flag;
    }

    /**
     * 查询
     * @return
     */
    public static List<Map<String, Object>> querySql(Connection connection, String sql, List<Object> params) throws SQLException {
        if (StringUtils.isBlank(sql)) {
            return Collections.emptyList();
        }
        List<Map<String, Object>> resultList = new ArrayList<>();
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        if (CollectionUtils.isNotEmpty(params)) {
            for (int i = 0; i < params.size(); i++) {
                preparedStatement.setObject(i + 1, params.get(i));
            }
        }
        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int cols = metaData.getColumnCount();
        while (resultSet.next()) {
            Map<String, Object> map = new HashMap<>();
            for (int i = 0; i < cols; i++) {
                String colName = metaData.getColumnLabel(i + 1);
                Object colValue = resultSet.getObject(colName);
                if (colValue != null) {
                    map.put(colName, colValue);
                }
            }
            resultList.add(map);
        }
        closeResource(preparedStatement, resultSet);
        return resultList;
    }

    public static void closeResource(Connection conn, PreparedStatement st, ResultSet rs) throws SQLException {
        if (rs != null) {
            rs.close();
        }
        if (st != null) {
            st.close();
        }

        if (conn != null) {
            conn.close();
        }
    }

    public static void closeResource(PreparedStatement st, ResultSet rs) throws SQLException {
        if (rs != null) {
            rs.close();
        }
        if (st != null) {
            st.close();
        }
    }

    public static void closeResource(PreparedStatement st) throws SQLException {
        if (st != null) {
            st.close();
        }
    }

    public static void main(String... args) throws Exception {
        Connection conn = getConnection("com.mysql.jdbc.Driver",
                "jdbc:mysql://haitao-jdb-fun-test-11107.rds.cn-east-p1.internal:3331/dist_star?useUnicode=true&characterEncoding=UTF-8",
                "dist_star",
                "$dist_star!#%");
        PreparedStatement preparedStatement = conn.prepareStatement("select buy_account as gorder_buy_account, gorder_time, benefit_user as gorder_benefit_user from dist_gorder where gorder_id = ?");
        preparedStatement.setObject(1, "201902121359GORDER32923335");
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            System.out.println();
        }
    }
}
