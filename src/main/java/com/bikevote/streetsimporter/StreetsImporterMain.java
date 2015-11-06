/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bikevote.streetsimporter;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
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
        streetsImporter.createDb();
        streetsImporter.doTheDance();
        streetsImporter.shutDown();

    }

    void createDb() {
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase("~/neo4j-native");
        registerShutdownHook(graphDb);

        
    }

    void doTheDance() {

        try (Transaction tx = graphDb.beginTx()) {

            for (int i = 1; i < 100000; i++) {
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
    }

    void shutDown() {
        System.out.println();
        System.out.println("Shutting down database ...");
        graphDb.shutdown();
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
