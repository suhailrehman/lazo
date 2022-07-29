package lazo.arrowserver.flightrpc;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import lazo.index.LazoIndex;
import lazo.index.LazoIndex.LazoCandidate;
import lazo.sketch.LazoSketch;

//Logging
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class LazoProducer extends NoOpFlightProducer {
    private final BufferAllocator allocator;
    private final Location location;
    private ConcurrentHashMap<String, SketchSet> hashes;
    private ConcurrentHashMap<String, HashSet<String>> uniqueValues;
    private ConcurrentHashMap<String, LazoSketch> sketchValues;
    private LazoIndex index;
    private static final Logger logger = LoggerFactory.getLogger(LazoProducer.class); // equiv to  LogManager.getLogger(MyTest.class);
    private float jcx_threshold;


    public LazoProducer(BufferAllocator allocator, Location location) {
        this.allocator = allocator;
        this.location = location;
        this.hashes = new ConcurrentHashMap<>();
        this.uniqueValues = new ConcurrentHashMap<>();
        this.sketchValues = new ConcurrentHashMap<>();
        this.index = new LazoIndex();
        this.jcx_threshold = 0.1f;
    }

    private String descriptorToString(FlightDescriptor fd){
        StringBuilder sb = new StringBuilder();
        for (String pathComponent: fd.getPath()){
            sb.append(pathComponent);
        }
        return sb.toString();
    }

    // generate a hash
    @Override
    public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        // List<ArrowRecordBatch> batches = new ArrayList<>();
        // FlightDescriptor fDescriptor = flightStream.getDescriptor();
        
        // Get the schema from the descriptor
        Schema schema = flightStream.getSchema();
        List<Field> fields = schema.getFields();

        logger.info("Storing: "+ descriptorToString(flightStream.getDescriptor()));
        logger.debug("Computing Lazo Sketch for Schema: " + schema.toString());
        logger.debug("Fields: " + fields.toString());

        SketchSet sketchSet = new SketchSet(schema);

        return () -> {
            long rows = 0;
            
            while (flightStream.next()) {
                VectorSchemaRoot root = flightStream.getRoot();
                VectorUnloader unloader = new VectorUnloader(root);

                // Unload the vector into a list of ArrowRecordBatch
                try (final ArrowRecordBatch arb = unloader.getRecordBatch()) {
                    for (FieldVector fieldVector : root.getFieldVectors()) {
                        String feildName = fieldVector.getField().getName();
                        // LazoSketch fieldSketch = new LazoSketch();
                        // HashSet<String> uniqueValues = new HashSet<String>();
                        HashSet<String> uniqueValueHashSet = uniqueValues.contains(feildName) ? uniqueValues.get(feildName) : new HashSet<String>();

                        for (int i=0; i < root.getRowCount(); i++){
                            String fieldContents = fieldVector.getObject(i).toString();
                            uniqueValueHashSet.add(fieldContents);
                        }
                        
                        uniqueValues.put(feildName, uniqueValueHashSet);
                    }
                    
                    rows += flightStream.getRoot().getRowCount();
                }
                catch (Exception e){
                    logger.error("Error while processing stream: " + e.getMessage());
                }
                
            }

            // Add the sketch to the sketch set
            for (Field field : schema.getFields()) {
                String feildName = field.getName();
                LazoSketch fieldSketch = sketchValues.contains(feildName) ? sketchValues.get(feildName) : new LazoSketch();
                for (String uniqueValue : uniqueValues.get(feildName)) {
                    fieldSketch.update(uniqueValue);
                }
                sketchValues.put(feildName, fieldSketch);
                sketchSet.setSketch(feildName, fieldSketch);
            }

            hashes.put(descriptorToString(flightStream.getDescriptor()), sketchSet);
            
            logger.debug("S1: Server: Sketched "+ rows +" rows for Table: "+flightStream.getDescriptor());
            
            if (this.hashes.size() % 1000 == 0){
                logger.info("S1: Server: Sketched "+ this.hashes.size() +" column sets");
            }
            ackStream.onCompleted();

        };
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {

        FlightDescriptor flightDescriptor = FlightDescriptor.path(
                new String(ticket.getBytes(), StandardCharsets.UTF_8));
        SketchSet sketchSet = this.hashes.get(descriptorToString(flightDescriptor));
        if (sketchSet == null) {
            throw CallStatus.NOT_FOUND.withDescription("Unknown descriptor").toRuntimeException();
        } else {

            List<Field> allFields = new ArrayList<Field>();
            for (Field field: sketchSet.getSchema().getFields()){
                String fieldName = field.getName();
                Field sketchField = new Field(fieldName, FieldType.nullable(new ArrowType.Int(BigIntVector.TYPE_WIDTH, true)), null);
                allFields.add(sketchField);
            }
            
            Schema sketchSchema = new Schema(allFields);

            VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(sketchSchema, allocator);
            listener.start(vectorSchemaRoot);
            
            for (Field field: allFields){
                long[] hashvalues = sketchSet.getSketch(field.getName()).getHashValues();
                BigIntVector hashVector = (BigIntVector) vectorSchemaRoot.getVector(field);
                hashVector.allocateNew(hashvalues.length);
                for (int i=0; i< hashvalues.length; i++){
                    hashVector.set(i, hashvalues[i]);
                }

                vectorSchemaRoot.setRowCount(hashvalues.length);
            }
            
            try{
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchemaRoot, null, out);
                writer.start();
                writer.writeBatch();
                writer.end();
                writer.close();
            } catch (IOException e){
                e.printStackTrace();
            }

            listener.completed();

        }
        
    }

    @Override
    @SuppressWarnings("unchecked")
    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
        logger.debug("S1 Server: doAction called: Type:"+action.getType());
        FlightDescriptor flightDescriptor = FlightDescriptor.path(
                new String(action.getBody(), StandardCharsets.UTF_8));
        SketchSet sketchSet = hashes.get(descriptorToString(flightDescriptor));
        
        switch (action.getType()) {
            case "INDEX":
                logger.debug("S1 Server: doAction Indexing: "+flightDescriptor.getPath());    
                for (Field field: sketchSet.getSchema().getFields()){
                    String fieldName = field.getName();
                    String indexKey = fieldName;
                    index.insert(indexKey, sketchSet.getSketch(fieldName));
                    logger.debug("S1 Server: Indexed: "+fieldName);
                }
                Result indexResult = new Result("Added to Index".getBytes(StandardCharsets.UTF_8));
                listener.onNext(indexResult);
                listener.onCompleted();
                break;

            case "INDEXALL":
                logger.info("S1 Server: doAction Indexing All: "+flightDescriptor.getPath()); 
                index = new LazoIndex();
                int num_indexed = 0;
                for (SketchSet skSet: hashes.values()){   
                    logger.debug("Indexing"+skSet.toString());
                    for (Field field: skSet.getSchema().getFields()){
                        try{
                            String fieldName = field.getName();
                            String indexKey = fieldName;
                            logger.debug(fieldName+" loaded");
                            logger.debug("Sketch contents: "+ Arrays.toString(skSet.getSketch(fieldName).getHashValues()));
                            index.insert(indexKey, skSet.getSketch(fieldName));
                            num_indexed++;
                //          logger.debug("S1 Server: Indexed: "+fieldName);
                        } catch (Exception e){
                            logger.error(e.getMessage());
                        }
                    }
                }
                logger.info("S1 Server: Indexed "+num_indexed+" columns");
                Result indexAllResult = new Result("Added all Sketches to Index".getBytes(StandardCharsets.UTF_8));
                listener.onNext(indexAllResult);
                listener.onCompleted();
                break;

            case "THRESHOLD":
                logger.info("S1 Server: doAction Threshold: "+flightDescriptor.getPath());    
                String threshold = new String(action.getBody(), StandardCharsets.UTF_8);
                jcx_threshold = Float.parseFloat(threshold);
                Result thresholdResult = new Result(String.format("Threshold set to: %f",jcx_threshold).getBytes(StandardCharsets.UTF_8));
                listener.onNext(thresholdResult);
                listener.onCompleted();
                break;

            case "QUERY":
                logger.info("S1 Server: doAction Querying: "+flightDescriptor.getPath()+" with threshold: "+jcx_threshold);
                sketchSet = hashes.get(descriptorToString(flightDescriptor));
                
                StringJoiner sb = new StringJoiner("\n");

                for (LazoSketch lazoSketch: sketchSet.getAllSketches()){
                    logger.info("Query Sketch: " + lazoSketch.toString());
                    Set<LazoCandidate> indexesResult = index.queryContainment(lazoSketch, jcx_threshold);
                    for (LazoCandidate lc: indexesResult){
                        sb.add(lc.key.toString());
                    }
                }

                Result queryResult = new Result(sb.toString().getBytes(StandardCharsets.UTF_8));
                listener.onNext(queryResult); 
                listener.onCompleted();
                break;

            case "SERIALIZE":
                logger.info("S1 Server: Serializing Existing SketchSet to file: "+flightDescriptor.getPath());
                try{
                    ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(new File(descriptorToString(flightDescriptor))));
                    out.writeObject(this.hashes);
                    out.close();
                }
                catch (IOException e){
                    logger.error("Unable to serialize SketchSet to file: "+flightDescriptor.getPath());
                    e.printStackTrace();
                }

                Result serializeResult = new Result("Serialized to File: ".getBytes(StandardCharsets.UTF_8));
                listener.onNext(serializeResult); 
                listener.onCompleted();
                break;

            case "LOAD":
                logger.info("S1 Server: Loading SketchSet from file: "+flightDescriptor.getPath());
                try{
                    ObjectInputStream in = new ObjectInputStream(new FileInputStream(new File(descriptorToString(flightDescriptor))));
                    this.hashes = (ConcurrentHashMap<String,SketchSet>) in.readObject();
                    logger.info(String.format("S1 Server: Loaded %d SketchSets from file.", this.hashes.size()));
                }
                catch (IOException | ClassNotFoundException e){
                    logger.error("Unable to serialize SketchSet to file: "+flightDescriptor.getPath());
                    e.printStackTrace();
                }

                Result loadResult = new Result("Serialized to File: ".getBytes(StandardCharsets.UTF_8));
                listener.onNext(loadResult); 
                listener.onCompleted();
                break;
        }
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        FlightEndpoint flightEndpoint = new FlightEndpoint(
                new Ticket(descriptor.getPath().get(0).getBytes(StandardCharsets.UTF_8)), location);
        return new FlightInfo(
                hashes.get(descriptorToString(descriptor)).getSchema(),
                descriptor,
                Collections.singletonList(flightEndpoint),
                /*bytes=*/-1, 0
        );
    }

    @Override
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
        hashes.forEach((k, v) -> { listener.onNext(getFlightInfo(null, FlightDescriptor.path(k))); });
        listener.onCompleted();
    }

    public static void main(String[] args) {
        Location location = Location.forGrpcInsecure("0.0.0.0", 33333);
        try (BufferAllocator allocator = new RootAllocator()){
            // Server
            try(FlightServer flightServer = FlightServer.builder(allocator, location,
                    new LazoProducer(allocator, location)).build()) {
                try {
                    flightServer.start();
                    logger.info("S1: Server (Location): Listening on port " + flightServer.getPort());
                    logger.info("S1: Server (Location): Press CTRL-C to stop.");
                    flightServer.awaitTermination();

                } catch (IOException e) {
                    logger.error("S1: Server (Location): Could not start server.");
                    System.exit(1);
                }

            }
        } catch (InterruptedException e){
            e.printStackTrace();
            logger.info("S1: Server (Location): Exiting...");
        }
    }
}
