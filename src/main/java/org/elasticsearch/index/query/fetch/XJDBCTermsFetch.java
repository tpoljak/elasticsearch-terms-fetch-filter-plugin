package org.elasticsearch.index.query.fetch;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.cache.filter.support.CacheKeyFilter;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.query.QueryParseContext;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class XJDBCTermsFetch extends XTermsFetch {

    private final String username;
    private final String password;
    private final String query;

    public XJDBCTermsFetch(String url, String query, String driver, String username, String password, CacheKeyFilter.Key key,
                           FieldMapper fieldMapper, @Nullable QueryParseContext queryParseContext) throws ClassNotFoundException {
        super(url, key, fieldMapper, queryParseContext);
        Class.forName(driver);
        this.username = username;
        this.password = password;
        this.query = query;
    }

    @Override
    protected List<Object> fetchTerms() {
        List<Object> terms = new ArrayList<Object>();
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DriverManager.getConnection(url, username, password);
            stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                terms.add(rs.getObject(1));
            }
            rs.close();
        } catch (SQLException e) {
            throw new RuntimeException("Error while opening connection with database", e);
        } finally {
            try {
                if (stmt != null)
                    stmt.close();
            } catch (SQLException se) {
                // nothing to do
            }
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException se) {
                // nothing to do
            }
        }
        return terms;
    }
}
