/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bikevote.streetsimporter;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.morbz.osmonaut.EntityFilter;
import net.morbz.osmonaut.IOsmonautReceiver;
import net.morbz.osmonaut.Osmonaut;
import net.morbz.osmonaut.osm.Entity;
import net.morbz.osmonaut.osm.EntityType;
import net.morbz.osmonaut.osm.Tags;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

/**
 *
 * @author tobias
 */
public class StreetsImporterMain {

    GraphDatabaseService graphDb;
    Node firstNode;
    Node secondNode;
    Relationship relationship;
    JestClient jestClient;

    private static enum RelTypes implements RelationshipType {

        KNOWS
    }

    public static void main(String[] args) {
        try {
            // TODO code application logic here
            System.out.print("Helo there");
            //System.in.read();
        } catch (Exception ex) {
            Logger.getLogger(StreetsImporterMain.class.getName()).log(Level.SEVERE, null, ex);
        }

        StreetsImporterMain streetsImporter = new StreetsImporterMain();
        streetsImporter.scan();
        //streetsImporter.createDb();
        //streetsImporter.doTheDance();
        //streetsImporter.shutDown();

    }

    void createDb() {
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase("~/neo4j-native");
        registerShutdownHook(graphDb);

        HttpClientConfig clientConfig = new HttpClientConfig.Builder("http://elasticsearch.bikevote.com:9200").multiThreaded(true).build();
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(clientConfig);
        jestClient = factory.getObject();
        System.out.print("done the elastci");

    }

    void doTheDance() {

        try (Transaction tx = graphDb.beginTx()) {

            for (int i = 1; i < 10000; i++) {
                System.out.println("Count is: " + i);
                firstNode = graphDb.createNode();
                firstNode.setProperty("message", "Hello, ");
                secondNode = graphDb.createNode();
                secondNode.setProperty("message", "World!");

                relationship = firstNode.createRelationshipTo(secondNode, RelTypes.KNOWS);
                relationship.setProperty("message", "brave Neo4j ");

            }

            tx.success();
        }

        List<Index> actionList = new ArrayList<Index>();

        for (int i = 0; i < 100; i++) {
            Map<String, String> source = new LinkedHashMap<String, String>();
            source.put("owner_id", "kimchy_what");

            Index index = new Index.Builder(source).id("1").build();

            actionList.add(index);
        }

        Bulk bulk = new Bulk.Builder()
                .defaultIndex("test_nodes")
                .defaultType("node")
                .addAction(actionList)
                .build();

        try {
            jestClient.execute(bulk);
        } catch (IOException ex) {
            Logger.getLogger(StreetsImporterMain.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    void shutDown() {
        System.out.println();
        System.out.println("Shutting down database ...");
        graphDb.shutdown();
        jestClient.shutdownClient();
    }

    void scan() {
        // Set which OSM entities should be scanned (only nodes and ways in this case)
        EntityFilter filter = new EntityFilter(false, true, false);

        // Set the binary OSM source file
        Osmonaut naut = new Osmonaut("/home/tobias/Downloads/oberbayern-latest.osm.pbf", filter);

        // Start scanning by implementing the interface
        naut.scan(new IOsmonautReceiver() {
            
            @Override
            public boolean needsEntity(EntityType type, Tags tags) {
                // Only lakes with names
                //return (tags.hasKeyValue("natural", "water") && tags.hasKey("name"));
                
                return (
                        tags.hasKeyValue("highway", "primary") 
                        || 
                        tags.hasKeyValue("highway", "primary_link")
                        || 
                        tags.hasKeyValue("highway", "secondary")
                        || 
                        tags.hasKeyValue("highway", "secondary_link")
                        || 
                        tags.hasKeyValue("highway", "tertiary")
                        || 
                        tags.hasKeyValue("highway", "tertiary_link")
                        || 
                        tags.hasKeyValue("highway", "unclassified")
                        || 
                        tags.hasKeyValue("highway", "living_street")
                        || 
                        tags.hasKeyValue("highway", "track")
                        || 
                        tags.hasKeyValue("highway", "road")
                        || 
                        tags.hasKeyValue("highway", "cycleway")
                        );
                
            }


            @Override
            public void foundEntity(Entity entity) {
                // Print name and center coordinates
                String name = entity.getTags().get("name");
                System.out.println(name + ": " + entity.getCenter());
            }
        });
    }

    private static void registerShutdownHook(final GraphDatabaseService graphDb) {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                graphDb.shutdown();
            }
        });
    }

}
