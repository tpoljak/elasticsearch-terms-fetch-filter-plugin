package org.elasticsearch.index.query.fetch;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.cache.filter.support.CacheKeyFilter;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.query.QueryParseContext;

import java.util.List;

public abstract class XTermsFetch {

    private final FieldMapper fieldMapper;

    private final QueryParseContext queryParseContext;

    protected CacheKeyFilter.Key key;

    protected List<Object> terms;

    protected final String url;

    public XTermsFetch(String url, CacheKeyFilter.Key key, FieldMapper fieldMapper, @Nullable QueryParseContext queryParseContext) {
        this.url = url.toLowerCase(); // in case might be used as a key
        this.key = key;
        this.fieldMapper = fieldMapper;
        this.queryParseContext = queryParseContext;
    }

    public List<Object> getTerms() {
        if (terms == null) {
            terms = fetchTerms();
        }
        return terms;
    }

    /**
     * Fetches terms from the underlying source
     *
     * @return
     */
    protected abstract List<Object> fetchTerms();

    /**
     * Returns cache key that is generated based on the content of the terms fetched
     *
     * @return
     */
    public final CacheKeyFilter.Key cacheKey() {
        if (key == null) {
            List<Object> terms = getTerms();
            StringBuilder builder = new StringBuilder();
            for (Object term : terms)
                builder.append(term + ",");
            key = new CacheKeyFilter.Key(builder.toString());
        }
        return key;
    }

    public FieldMapper getFieldMapper() {
        return fieldMapper;
    }

    public QueryParseContext getQueryParseContext() {
        return queryParseContext;
    }

}
