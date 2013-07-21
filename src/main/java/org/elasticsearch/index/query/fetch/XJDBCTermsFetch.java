package org.elasticsearch.index.query.fetch;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.cache.filter.support.CacheKeyFilter;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.query.QueryParseContext;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class XJDBCTermsFetch extends XTermsFetch {

    private static final Map<String, BoneCP> pools = new HashMap<String, BoneCP>();
    private final String poolKey;
    private final String query;

    @Inject
    public XJDBCTermsFetch(String url, String query, String driver, String username, String password, CacheKeyFilter.Key key,
                           FieldMapper fieldMapper, @Nullable QueryParseContext queryParseContext) throws ClassNotFoundException, SQLException {
        super(url, key, fieldMapper, queryParseContext);
        this.poolKey = createPoolKey(url, driver);
        this.query = query;
        initializePool(driver, url, username, password);
    }

    @Override
    protected List<Object> fetchTerms() {
        List<Object> terms = new ArrayList<Object>();
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = pools.get(this.poolKey).getConnection();
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

    // TODO: think about implications...
    private String createPoolKey(String url, String driver) {
        return (url.toLowerCase() + driver.toLowerCase());
    }

    private void initializePool(String driver, String url, String username, String password) throws ClassNotFoundException, SQLException {
        if (!pools.containsKey(this.poolKey)) {
            synchronized (pools) {
                if (!pools.containsKey(this.poolKey)) {
                    Class.forName(driver);
                    BoneCPConfig config = new BoneCPConfig();
                    config.setJdbcUrl(url);
                    config.setUsername(username);
                    config.setPassword(password);
                    config.setMaxConnectionsPerPartition(20); // TODO: bring to config
                    pools.put(this.poolKey, new BoneCP(config));
                }
            }
        }
    }
}