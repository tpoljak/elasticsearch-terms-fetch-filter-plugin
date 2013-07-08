package org.elasticsearch.common.lucene.search;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.query.fetch.*;

import java.io.IOException;

public class XTermsFilter extends Filter {

    private static ESLogger logger = Loggers.getLogger(XTermsFilter.class);

    private final XTermsFetch fetch;

    private Filter filter;

    public XTermsFilter(XTermsFetch fetch) {
        this.fetch = fetch;
    }

    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        return getFilter().getDocIdSet(context, acceptDocs);
    }

    private Filter getFilter() {
        if (this.filter == null) {
            this.filter = fetch.getFieldMapper().termsFilter(fetch.getTerms(), fetch.getQueryParseContext());
        } 
        return filter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        XTermsFilter that = (XTermsFilter) o;
        if (fetch != null ? !fetch.equals(that.fetch) : that.fetch != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return fetch.hashCode();
    }
}