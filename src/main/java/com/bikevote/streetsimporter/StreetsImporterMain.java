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
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.StreamSupport;
import net.morbz.osmonaut.EntityFilter;
import net.morbz.osmonaut.IOsmonautReceiver;
import net.morbz.osmonaut.Osmonaut;
import net.morbz.osmonaut.osm.Entity;
import net.morbz.osmonaut.osm.EntityType;
import net.morbz.osmonaut.osm.Tags;
import net.morbz.osmonaut.osm.Way;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;

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

    int nodeCounter = 0;

    List<org.neo4j.graphdb.Node> bulkNodeList;

    private static enum RelTypes implements RelationshipType {

        ROAD
    }

    public static float distanceKm(float lat1, float lng1, float lat2, float lng2) {
        double earthRadius = 6371; //km
        double dLat = Math.toRadians(lat2 - lat1);
        double dLng = Math.toRadians(lng2 - lng1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLng / 2) * Math.sin(dLng / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        float dist = (float) (earthRadius * c);

        return dist;
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
        streetsImporter.createDb();

        streetsImporter.scan();

        streetsImporter.shutDown();

    }

    void createDb() {

        bulkNodeList = new ArrayList<>();

        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase("~/neo4j-native4");
        registerShutdownHook(graphDb);

        HttpClientConfig clientConfig = new HttpClientConfig.Builder("http://elasticsearch.bikevote.com:9200").multiThreaded(true).build();
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(clientConfig);
        jestClient = factory.getObject();
        System.out.print("done the elastci");

        try (Transaction tx = graphDb.beginTx()) {
            Schema schema = graphDb.schema();
            IndexDefinition osmIdIndex = schema.indexFor(DynamicLabel.label("RoadNode")).on("osm_id").create();
            tx.success();
        } catch (Exception ex) {
            System.out.printf("index already there, ok.");
            Logger.getLogger(StreetsImporterMain.class.getName()).log(Level.SEVERE, null, ex);
        }

        try (Transaction tx = graphDb.beginTx()) {
            Schema schema = graphDb.schema();
            IndexDefinition bikevoteIdIndex = schema.indexFor(DynamicLabel.label("RoadNode")).on("bikevote_id").create();
            tx.success();
        } catch (Exception ex) {
            System.out.printf("index already there, ok.");
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
        Osmonaut naut = new Osmonaut("/home/tobias/Downloads/germany-latest.osm.pbf", filter);

        // Start scanning by implementing the interface
        naut.scan(new IOsmonautReceiver() {

            private Node currentNode;
            private Node nextNode;

            @Override
            public boolean needsEntity(EntityType type, Tags tags) {
                // Only lakes with names
                //return (tags.hasKeyValue("natural", "water") && tags.hasKey("name"));

                return (tags.hasKeyValue("highway", "primary")
                        || tags.hasKeyValue("highway", "primary_link")
                        || tags.hasKeyValue("highway", "secondary")
                        || tags.hasKeyValue("highway", "secondary_link")
                        || tags.hasKeyValue("highway", "tertiary")
                        || tags.hasKeyValue("highway", "tertiary_link")
                        || tags.hasKeyValue("highway", "unclassified")
                        || tags.hasKeyValue("highway", "living_street")
                        || tags.hasKeyValue("highway", "track")
                        || tags.hasKeyValue("highway", "road")
                        || tags.hasKeyValue("highway", "cycleway"));

            }

            @Override
            public void foundEntity(Entity entity) {
                // Print name and center coordinates
                //String name = entity.getTags().get("name");
                //System.out.println(name + ": " + entity.getCenter());
                List<net.morbz.osmonaut.osm.Node> entityList = ((Way) entity).getNodes();

                Label roadNodeLabel = DynamicLabel.label("RoadNode");

                try (Transaction tx = graphDb.beginTx()) {

                    entityList.stream().forEach((node) -> {
                        nodeCounter++;
                        System.out.println("Node-Counter " + nodeCounter);

                        ResourceIterator<Node> nodes = graphDb.findNodes(roadNodeLabel, "osm_id", node.getId());
                        if (nodes.hasNext()) {
                            //System.out.println("OSM-Node already there!!!");
                            nextNode = nodes.next();
                            if (nodes.hasNext()) {
                                System.out.print("we had a double node osm id!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                                throw new RuntimeException("This should not happen!!!!");
                            }
                        } else {
                            String uniqueID = UUID.randomUUID().toString();
                            nextNode = graphDb.createNode(DynamicLabel.label("RoadNode"));
                            nextNode.setProperty("bikevote_id", uniqueID);
                            nextNode.setProperty("osm_id", node.getId());
                        }

                        nodes.close();

                        nextNode.setProperty("lat", node.getLatlon().getLat());
                        nextNode.setProperty("lon", node.getLatlon().getLon());
                        nextNode.setProperty("owner", "osm");

                        Tags nextTags = node.getTags();
                        if (nextTags.hasKey("ele")) {
                            System.out.println("foudn elevation");
                            try {
                                nextNode.setProperty("elevation", Math.round(Float.parseFloat(nextTags.get("ele"))));
                            } catch (NumberFormatException ex) {
                                System.out.print("number of altitude could not be parsed.");
                            }
                        }

                        if (currentNode != null) {
                            if (StreamSupport.stream(currentNode.getRelationships().spliterator(), false).filter(
                                    relationship
                                    -> relationship.getStartNode().equals(nextNode) || relationship.getEndNode().equals(nextNode)
                            ).count() > 0) {
                                System.out.println("relationship already exists.");
                            } else {
                                Relationship relationship = currentNode.createRelationshipTo(nextNode, RelTypes.ROAD);
                                float distance = distanceKm(
                                        currentNode.getProperty("lat"),
                                        currentNode.getProperty("lon"),
                                        nextNode.getProperty("lat"),
                                        nextNode.getProperty("lon"));

                                relationship.setProperty("length_km", distance);
                                relationship.setProperty("voted_weight", distance);
                                
                                if (currentNode.hasProperty("elevation") && nextNode.hasProperty("elevation")){
                                    int eleDiff = Integer.parseInt(nextNode.getProperty("elevation").toString()) - Integer.parseInt(currentNode.getProperty("elevation").toString());
                                    relationship.setProperty("elevation_diff", eleDiff);
                                }
                            }
                        }

                        currentNode = nextNode;

                        //indexNodeWithElasticsearch(nextNode);
                    });

                    tx.success();
                }

            }
        });
    }

    private void indexNodeWithElasticsearch(Node node) {
        //System.out.println(node.getId());

        bulkNodeList.add(node);

        if (bulkNodeList.size() > 100000) {
            flushNodesToElasticsearch();
        }
    }

    private void flushNodesToElasticsearch() {
        System.out.println("flushing nodes to elasticsweach....................................................");
        List<Index> actionList = new ArrayList<>();

        bulkNodeList.stream().forEach((node) -> {
            Map<String, String> source = new LinkedHashMap<>();
            source.put("owner_id", "kimchy_what");
            Index index = new Index.Builder(source).id("1").build();
            actionList.add(index);
        });

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

        bulkNodeList = new ArrayList<>();
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
