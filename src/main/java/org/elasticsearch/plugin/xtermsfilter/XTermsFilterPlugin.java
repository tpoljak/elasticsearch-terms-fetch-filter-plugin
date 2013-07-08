package org.elasticsearch.plugin.xtermsfilter;

import org.elasticsearch.index.query.IndexQueryParserModule;
import org.elasticsearch.index.query.XTermsFilterParser;
import org.elasticsearch.plugins.AbstractPlugin;

public class XTermsFilterPlugin extends AbstractPlugin {

    @Override
    public String name() {
        return "xterms-filter";
    }

    @Override
    public String description() {
        return "a filter plugin for ElasticSearch that fetches values for a terms filter from an external data sourcesource";
    }

    public void onModule(IndexQueryParserModule module) {
        module.addFilterParser(XTermsFilterParser.NAME, XTermsFilterParser.class);
    }
}
