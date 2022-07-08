package lazo.arrowserver.flightrpc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
    private final ConcurrentHashMap<FlightDescriptor, SketchSet> hashes;
    private final LazoIndex index;
    private static final Logger logger = LoggerFactory.getLogger(LazoProducer.class); // equiv to  LogManager.getLogger(MyTest.class);
    private float jcx_threshold;


    public LazoProducer(BufferAllocator allocator, Location location) {
        this.allocator = allocator;
        this.location = location;
        this.hashes = new ConcurrentHashMap<>();
        this.index = new LazoIndex();
        this.jcx_threshold = 0.5f;
    }

    // generate a hash
    @Override
    public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        // List<ArrowRecordBatch> batches = new ArrayList<>();
        // FlightDescriptor fDescriptor = flightStream.getDescriptor();
        
        // Get the schema from the descriptor
        Schema schema = flightStream.getSchema();
        List<Field> fields = schema.getFields();

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
                        LazoSketch fieldSketch = new LazoSketch();
                        HashSet<String> uniqueValues = new HashSet<String>();
                        for (int i=0; i < root.getRowCount(); i++){
                            String fieldContents = fieldVector.getObject(i).toString();
                            if (uniqueValues.add(fieldContents)) {
                                fieldSketch.update(fieldContents);
                            }
                        }
                        sketchSet.setSketch(fieldVector.getField().getName(), fieldSketch);
                    }
                    
                    rows += flightStream.getRoot().getRowCount();
                }
                
            }

            hashes.put(flightStream.getDescriptor(), sketchSet);
            
            logger.debug("S1: Server: Sketched "+ rows +" rows for Table: "+flightStream.getDescriptor());
            ackStream.onCompleted();

        };
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {

        FlightDescriptor flightDescriptor = FlightDescriptor.path(
                new String(ticket.getBytes(), StandardCharsets.UTF_8));
        SketchSet sketchSet = this.hashes.get(flightDescriptor);
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
    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
        logger.debug("S1 Server: doAction called: Type:"+action.getType());
        FlightDescriptor flightDescriptor = FlightDescriptor.path(
                new String(action.getBody(), StandardCharsets.UTF_8));
        SketchSet sketchSet = hashes.get(flightDescriptor);
        
        switch (action.getType()) {
            case "INDEX":
                logger.debug("S1 Server: doAction Indexing: "+flightDescriptor.getPath());    
                for (Field field: sketchSet.getSchema().getFields()){
                    String fieldName = field.getName();
                    String indexKey = fieldName;
                    index.update(indexKey, sketchSet.getSketch(fieldName));
                    logger.debug("S1 Server: Indexed: "+fieldName);
                }
                Result indexResult = new Result("Added to Index".getBytes(StandardCharsets.UTF_8));
                listener.onNext(indexResult);
                listener.onCompleted();
                break;

            case "THRESHOLD":
                logger.debug("S1 Server: doAction Threshold: "+flightDescriptor.getPath());    
                String threshold = new String(action.getBody(), StandardCharsets.UTF_8);
                jcx_threshold = Float.parseFloat(threshold);
                Result thresholdResult = new Result(String.format("Threshold set to: %f",jcx_threshold).getBytes(StandardCharsets.UTF_8));
                listener.onNext(thresholdResult);
                listener.onCompleted();
                break;

            case "QUERY":
                logger.debug("S1 Server: doAction Querying: "+flightDescriptor.getPath());
                sketchSet = hashes.get(flightDescriptor);
                
                StringJoiner sb = new StringJoiner("\n");

                for (LazoSketch lazoSketch: sketchSet.getAllSketches()){
                    Set<LazoCandidate> indexesResult = index.queryContainment(lazoSketch, jcx_threshold);
                    for (LazoCandidate lc: indexesResult){
                        sb.add(lc.key.toString());
                    }
                }

                Result queryResult = new Result(sb.toString().getBytes(StandardCharsets.UTF_8));
                listener.onNext(queryResult); 
                listener.onCompleted();
                break;
        }
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        FlightEndpoint flightEndpoint = new FlightEndpoint(
                new Ticket(descriptor.getPath().get(0).getBytes(StandardCharsets.UTF_8)), location);
        return new FlightInfo(
                hashes.get(descriptor).getSchema(),
                descriptor,
                Collections.singletonList(flightEndpoint),
                /*bytes=*/-1, 0
        );
    }

    @Override
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
        hashes.forEach((k, v) -> { listener.onNext(getFlightInfo(null, k)); });
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
                    System.exit(1);
                }

            }
        } catch (InterruptedException e){
            e.printStackTrace();
            logger.info("S1: Server (Location): Exiting...");
        }
    }
}