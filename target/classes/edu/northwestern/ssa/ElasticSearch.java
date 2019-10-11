package edu.northwestern.ssa;

import org.json.JSONObject;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpMethod;

import java.io.IOException;
import java.util.Optional;

class ElasticSearch extends AwsSignedRestRequest{
    ElasticSearch(String serviceName) {
        super(serviceName);
    }

    int createIndex(String host, String index) throws IOException {
        HttpExecuteResponse res = this.restRequest(SdkHttpMethod.PUT, host, index, Optional.empty());

        res.responseBody().get().close();

        return res.httpResponse().statusCode();
    }

    int postDocument(String host, String path, JSONObject document) throws IOException {

        HttpExecuteResponse res  = this.restRequest(SdkHttpMethod.POST, host, path + "/_doc", Optional.empty(), Optional.of(document));

        res.responseBody().get().close();

        return res.httpResponse().statusCode();
    }

    int  searchDocuments() {
        return 0;
    }
}
