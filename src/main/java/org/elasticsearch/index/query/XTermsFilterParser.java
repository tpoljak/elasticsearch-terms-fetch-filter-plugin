package org.elasticsearch.index.query;

import org.apache.lucene.search.Filter;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.lucene.search.XTermsFilter;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.filter.support.CacheKeyFilter;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.fetch.*;

import java.io.IOException;
import java.util.ArrayList;

public class XTermsFilterParser implements FilterParser {

    public static final String NAME = "xterms";

    @Inject
    public XTermsFilterParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME, "in"};
    }

    @Override
    public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();
        MapperService.SmartNameFieldMappers smartNameFieldMappers;

        String filterName = null;
        String currentFieldName = null;

        // fetch settings
        // common to all
        String url = null;
        Integer timeout = null;
        CacheKeyFilter.Key cacheKey = null;
        boolean cache = true;

        // rest fetch
        String path = null;

        // redis fetch
        String command = null;
        ArrayList<String> args = new ArrayList<String>();

        // jdbc fetch
        String query = null;
        String driver = null;
        String username = null;
        String password = null;


        XContentParser.Token token;
        String fieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            }
            if (token == XContentParser.Token.START_OBJECT) {
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token.isValue()) {
                        if ("url".equals(currentFieldName)) {
                            url = parser.text();
                        } else if ("timeout".equals(currentFieldName)) {
                            timeout = parser.intValue();
                        }
                        // rest data source
                        else if ("path".equals(currentFieldName)) {
                            path = parser.text();
                        }
                        // database data source
                        else if ("query".equals(currentFieldName)) {
                            query = parser.text();
                        } else if ("driver".equals(currentFieldName)) {
                            driver = parser.text();
                        } else if ("username".equals(currentFieldName)) {
                            username = parser.text();
                        } else if ("password".equals(currentFieldName)) {
                            password = parser.text();
                        }
                        // redis datasource
                        else if ("command".equals(currentFieldName)) {
                            command = parser.text();
                        } else {
                            throw new QueryParsingException(parseContext.index(), "[xterms] filter does not support [" + currentFieldName + "] within lookup element");
                        }
                    } else if (token == XContentParser.Token.START_ARRAY) {
                        if ("args".equals(currentFieldName)) {
                            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                String value = parser.text();
                                if (value == null) {
                                    throw new QueryParsingException(parseContext.index(), "No arguments specified for redis command");
                                }
                                args.add(value);
                            }
                        } else {
                            throw new QueryParsingException(parseContext.index(), "[xterms] filter does not support array for parameter [" + currentFieldName + "]");
                        }
                    }
                }
            } else if (token.isValue()) {
                if ("_name".equals(currentFieldName)) {
                    filterName = parser.text();
                } else if ("_cache".equals(currentFieldName)) {
                    cache = parser.booleanValue();
                } else if ("_cache_key".equals(currentFieldName) || "_cacheKey".equals(currentFieldName)) {
                    cacheKey = new CacheKeyFilter.Key(parser.text());
                } else {
                    throw new QueryParsingException(parseContext.index(), "[xterms] filter does not support [" + currentFieldName + "]");
                }
            }
        }

        // validation
        if (fieldName == null) {
            throw new QueryParsingException(parseContext.index(), "[xterms] filter requires a field name and the definition of where the terms should be fetched from");
        }
        if (url == null) {
            throw new QueryParsingException(parseContext.index(), "[xterms] filter requires specifying an [url]");
        }

        FieldMapper fieldMapper = null;
        smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);
        if (smartNameFieldMappers != null) {
            if (smartNameFieldMappers.hasMapper()) {
                fieldMapper = smartNameFieldMappers.mapper();
            }
        }

        // if there are no mappings, then nothing has been indexing yet against this shard, so we can return
        // no match (but not cached!), since the Terms Lookup relies on the fact that there are mappings...
        if (fieldMapper == null) {
            return Queries.MATCH_NO_FILTER;
        }

        XTermsFetch termsFetch = null;
        if (query != null) {
            validateJDBCFetch(parseContext.index(), url, query, driver, username, password);
            try {
                termsFetch = new XJDBCTermsFetch(url, query, driver, username, password, cacheKey, fieldMapper, parseContext);
            } catch (ClassNotFoundException e) {
                throw new QueryParsingException(parseContext.index(), "[xterms] driver [" + driver + "] was not found in classpath");
            }
        } else if (command != null) {
            validateRedisFetch(parseContext.index(), url, command, args);
            termsFetch = new XRedisTermsFetch(url, command, args, cacheKey, fieldMapper, parseContext);
        } else {
            validateRestFetch(parseContext.index(), url, path);
            termsFetch = new XRestTermsFetch(url, path, timeout, cacheKey, fieldMapper, parseContext);
        }
        // fetch terms definition

        Filter filter = new XTermsFilter(termsFetch);
        // cache the whole filter by default, or if explicitly told to
        if (cache) {
            filter = parseContext.cacheFilter(filter, termsFetch.cacheKey());
        }
        if (filterName != null) {
            parseContext.addNamedFilter(filterName, filter);
        }
        return filter;
    }

    private void validateRedisFetch(Index index, String url, String command, ArrayList<String> args) {
        if (command == null || args.size() == 0) {
            throw new QueryParsingException(index, "[xterms] filter redis lookup element requires a [command] and an [args] parameter. args must be a non empty array");
        }
    }

    private void validateJDBCFetch(Index index, String url, String query, String driver, String username, String password) {
        if (query == null || driver == null || username == null || password == null) {
            throw new QueryParsingException(index, "[xterms] filter jdbc lookup element requires all of [url, query, driver, username, password]");
        }
    }

    private void validateRestFetch(Index index, String url, String path) {
        // none so far
    }
}
