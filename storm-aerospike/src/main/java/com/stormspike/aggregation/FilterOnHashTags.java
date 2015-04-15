package com.stormspike.aggregation;


import com.aerospike.client.query.Filter;
import com.aerospike.client.query.Statement;

import java.util.List;

public class FilterOnHashTags {

    private String namespace;
    private String set;
    private List<String> hashTags;

    public FilterOnHashTags(String namespace, String set) {
        this.namespace = namespace;
        this.set = set;
    }

    public Statement aggregate() {
        Statement stmt = new Statement();
        stmt.setNamespace(this.namespace);
        stmt.setSetName(this.set);
        stmt.setFilters(Filter.equal("hashTag", "jobs"));
        stmt.setBinNames("hashTag");
        return stmt;
    }

}
