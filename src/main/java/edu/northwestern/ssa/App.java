package edu.northwestern.ssa;


import org.apache.commons.lang.ObjectUtils;
import org.json.JSONObject;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import org.archive.io.*;
import org.archive.io.warc.*;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class App {
    // step1
    private static int download(String fileName)
    {
        S3Client s3 = S3Client.builder()
                .region(Region.US_EAST_1)
                .overrideConfiguration(ClientOverrideConfiguration.builder().apiCallTimeout(Duration.ofMinutes(30)).build())
                .build();
        GetObjectRequest request = GetObjectRequest.builder()
                                                    .bucket("commoncrawl")
                                                    .key(fileName)
                                                    .build();
        File f = new File("./test.warc.gz");
        s3.getObject(request, ResponseTransformer.toFile(f));
        s3.close();
//        final S3AsyncClient s3 = S3AsyncClient.builder()
//                .region(Region.US_EAST_1)
//                .overrideConfiguration(ClientOverrideConfiguration.builder()
//                        .apiCallTimeout(Duration.ofMinutes(30)).build())
//                .build();
//        GetObjectRequest rq = GetObjectRequest.builder()
//                .bucket("commoncrawl")
//                .key(fileName)
//                .build();
//        final String TMP_FILE = "test.warc.gz";
//        File f = new File(TMP_FILE);
//        f.delete();
//        f = new File(TMP_FILE);
//        CompletableFuture<GetObjectResponse> resp = s3.getObject(rq, AsyncResponseTransformer.toFile(f));
//        while(!resp.isDone()) {
//            try {
//                Thread.sleep(1000);  // one second
//            } catch (InterruptedException e) {}
//        }
//        s3.close();
//
        return 1;
    }
    // step2,3,4
    private static boolean parseFile(String host, String index) throws IOException, InterruptedException {
        Path path = Paths.get("test.warc.gz");
        File f = new File("test.warc.gz");
        InputStream is = Files.newInputStream(path);
        //ArchiveReader reader = WARCReaderFactory.get("test.warc.gz", is , true);
        ArchiveReader reader = WARCReaderFactory.get(f);
        int count = 1;

        final ExecutorService pool = Executors.newFixedThreadPool(80);

        for (ArchiveRecord record : reader) {
            String url = record.getHeader().getUrl();

            if (record.getHeader().getMimetype().equals("application/http; msgtype=response")) {
                //if (url.contains(".pdf")) continue;

//                int n = record.read();
//                StringBuilder temp = new StringBuilder();
//                while (n != -1){
//                    temp.append((char)n);
//                    n = record.read();
//                }
//                String s = new String(temp);
                try {
                    byte[] bytesArray = IOUtils.toByteArray(record, record.available());
                    String s = new String(bytesArray);
                    String [] splitRes = s.split("\\r\\n\\r\\n", 2);
                    if (splitRes.length < 2) {
                        continue;
                    }

                    Document doc = Jsoup.parse(splitRes[1]);

                    JSONObject document = new JSONObject();

                    document.put("title", doc.title());
                    document.put("txt", doc.text());
                    document.put("url", url);

                    pool.execute(() -> {
                        while (true) {
                            ElasticSearch es = new ElasticSearch("es");
                            int responseCode = 0;
                            try {
                                responseCode = es.postDocument(host, index, document);
                                es.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            if (responseCode == 201) {
                                break;
                            }
                        }
                    });
                } catch (Exception e) {

                }

                if(count % 100 == 0) {
                    System.out.println(count);
                }
                count++;
            }
        }
        reader.close();
        System.out.println("reader cloesed, with for thread ending");
        Thread.sleep(250000);
        pool.shutdown();

        return pool.awaitTermination(2, TimeUnit.MINUTES);
    }

    private static String fetchLatestFileName() {
        String bucketName = "commoncrawl";

        S3Client s3 = S3Client.builder()
                .region(Region.US_EAST_1)
                .overrideConfiguration(ClientOverrideConfiguration.builder().apiCallTimeout(Duration.ofMinutes(30)).build())
                .build();

        ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request
                .builder()
                .bucket(bucketName)
                .prefix("crawl-data/CC-NEWS/2019/10")
                .build();

        ListObjectsV2Response listObjectsV2Response = s3.listObjectsV2(listObjectsV2Request);

        List<S3Object> objectListing = listObjectsV2Response.contents();

        return objectListing.get(objectListing.size()-1).key();
    }

    public static Boolean CheckFile() {
        File tempFile = new File("test.warc.gz");
        return tempFile.exists();
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        String host = System.getenv("ELASTIC_SEARCH_HOST");
        String index = System.getenv("ELASTIC_SEARCH_INDEX");
        String fileName = System.getenv("COMMON_CRAWL_FILENAME");


        ElasticSearch es = new ElasticSearch("es");
        es.createIndex(host, index);

        //if (!CheckFile()) {
        if (fileName == null) {
            fileName = fetchLatestFileName();
        }
        System.out.println("Begin downloading...");
        int download_res = App.download(fileName); //step1
        while(true) {
            if (download_res == 1) {
                break;
            }
        }
        //}
        System.out.println("Step1 Finished");
        if (!App.parseFile(host, index)) {
            System.out.println("thread pool did not exist as expected");
            System.exit(0);
        }

    }
}
