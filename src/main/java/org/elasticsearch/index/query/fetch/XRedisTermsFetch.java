package org.elasticsearch.index.query.fetch;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.cache.filter.support.CacheKeyFilter;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.query.QueryParseContext;
import redis.clients.jedis.JedisAdaptor;

import java.util.ArrayList;
import java.util.List;

public class XRedisTermsFetch extends XTermsFetch {

    private final String cmd;
    private final ArrayList<String> args;

    public XRedisTermsFetch(String url, String cmd, ArrayList<String> args, CacheKeyFilter.Key key, FieldMapper fieldMapper, @Nullable QueryParseContext queryParseContext) {
        super(url, key, fieldMapper, queryParseContext);
        this.cmd = cmd.toLowerCase();
        this.args = args;
    }

    @Override
    protected List<Object> fetchTerms() {
        return JedisAdaptor.execute(url, cmd, args.toArray(new String[args.size()]));
    }

}