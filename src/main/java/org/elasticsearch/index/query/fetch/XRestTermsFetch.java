package org.elasticsearch.index.query.fetch;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.cache.filter.support.CacheKeyFilter;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.query.QueryParseContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class XRestTermsFetch extends XTermsFetch {

    private static final int DEFAULT_TIMEOUT = 300; // ms

    private final URL url;
    private final String path;
    private final int timeout;

    public XRestTermsFetch(String url, String path, Integer timeout, CacheKeyFilter.Key key, FieldMapper fieldMapper,
                           @Nullable QueryParseContext queryParseContext) throws MalformedURLException {
        super(url, key, fieldMapper, queryParseContext);
        this.url = new URL(url);
        this.path = path;
        this.timeout = timeout != null ? timeout : DEFAULT_TIMEOUT;
    }

    @Override
    protected List<Object> fetchTerms() {
        long start = System.currentTimeMillis();
        List<Object> terms = new ArrayList<Object>();
        InputStream is = null;
        try {
            URLConnection urlConnection = url.openConnection();
            urlConnection.setReadTimeout(timeout);
            urlConnection.setRequestProperty("Accept", "application/json");
            is = urlConnection.getInputStream();
            if (path == null) {
                XContentParser parser = JsonXContent.jsonXContent.createParser(is);
                if (parser.nextToken() != XContentParser.Token.START_ARRAY) {
                    throw new IllegalStateException("Error reading terms. Path doesnt contain a JSON array");
                }
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    terms.add(parser.text());
                }
            } else {
                byte[] content = toByteArray(is);
                Map<String, Object> source = XContentHelper.convertToMap(content, false).v2();
                terms = XContentMapValues.extractRawValues(path, source);
            }
            is.close();
        } catch (SocketTimeoutException ste) {
            throw new RuntimeException("Timeout of [" + timeout + "] reached when getting data from [" + url.toExternalForm() + "]");
        } catch (IOException e) {
            throw new RuntimeException("Error reading terms from [" + url.toExternalForm() + "]", e);
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    // nothing to be done
                }
            }
        }
        return terms;
    }

    private byte[] toByteArray(InputStream input) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024 * 4];
        int n;
        while (-1 != (n = input.read(buffer))) {
            output.write(buffer, 0, n);
        }
        return output.toByteArray();
    }

}