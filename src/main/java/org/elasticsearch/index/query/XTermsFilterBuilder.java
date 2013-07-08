package org.elasticsearch.index.query;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public class XTermsFilterBuilder extends BaseFilterBuilder {

    // common
    private final String name;
    private String url;
    private Integer timeout;
    private Boolean cache;
    private String cacheKey;
    private String filterName;

    // rest
    private String path;

    // jdbc
    private String query;
    private String driver;
    private String username;
    private String password;

    // redis
    private String command;
    private List<String> args;

    public XTermsFilterBuilder(String name) {
        this.name = name;
    }

    public XTermsFilterBuilder filterName(String filterName) {
        this.filterName = filterName;
        return this;
    }

    public XTermsFilterBuilder cacheKey(String cacheKey) {
        this.cacheKey = cacheKey;
        return this;
    }

    public XTermsFilterBuilder cache(boolean cache) {
        this.cache = cache;
        return this;
    }

    public XTermsFilterBuilder timeout(int timeout) {
        this.timeout = timeout;
        return this;
    }

    public XTermsFilterBuilder path(String path) {
        this.path = path;
        return this;
    }

    public XTermsFilterBuilder url(String url) {
        this.url = url;
        return this;
    }

    public XTermsFilterBuilder query(String query) {
        this.query = query;
        return this;
    }

    public XTermsFilterBuilder driver(String driver) {
        this.driver = driver;
        return this;
    }

    public XTermsFilterBuilder username(String username) {
        this.username = username;
        return this;
    }

    public XTermsFilterBuilder password(String password) {
        this.password = password;
        return this;
    }

    public XTermsFilterBuilder command(String command) {
        this.command = command;
        return this;
    }

    public XTermsFilterBuilder args(List<String> args) {
        this.args = args;
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(XTermsFilterParser.NAME);
        builder.startObject(name);
        builder.field("url", url);
        // rest
        if (path != null) {
            builder.field("path", path);
        }
        // jdbc
        if (query != null) {
            builder.field("query", query);
        }
        if (driver != null) {
            builder.field("driver", driver);
        }
        if (username != null) {
            builder.field("username", username);
        }
        if (password != null) {
            builder.field("password", password);
        }
        // redis
        if (command != null) {
            builder.field("command", command);
        }
        if (args != null) {
            builder.array("args", args);
        }
        //common
        if (timeout != null) {
            builder.field("timeout", timeout);
        }
        builder.endObject();
        if (filterName != null) {
            builder.field("_name", filterName);
        }
        if (cache != null) {
            builder.field("_cache", cache);
        }
        if (cacheKey != null) {
            builder.field("_cache_key", cacheKey);
        }
        builder.endObject();
    }
}