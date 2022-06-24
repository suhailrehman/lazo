package lazo.arrowserver.flightrpc;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Set;
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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import lazo.index.LazoIndex;
import lazo.index.LazoIndex.LazoCandidate;
import lazo.sketch.LazoSketch;


public class CookbookProducer extends NoOpFlightProducer {
    private final BufferAllocator allocator;
    private final Location location;
    private final ConcurrentHashMap<FlightDescriptor, Dataset> datasets;
    private final ConcurrentHashMap<String, LazoSketch> hashes;
    private final LazoIndex index;
    private final float jcx_threshold;

    public CookbookProducer(BufferAllocator allocator, Location location) {
        this.allocator = allocator;
        this.location = location;
        this.datasets = new ConcurrentHashMap<>();
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

        System.out.println("Indexing Schema: " + schema.toString());
        System.out.println("Fields: " + fields.toString());

        for (Field field : fields) {
            if (!hashes.contains(field.getName())) {
                hashes.put(field.getName(), new LazoSketch());
            }
        }
        
        return () -> {
            long rows = 0;
            
            while (flightStream.next()) {
                VectorSchemaRoot root = flightStream.getRoot();
                VectorUnloader unloader = new VectorUnloader(root);

                // Unload the vector into a list of ArrowRecordBatch
                try (final ArrowRecordBatch arb = unloader.getRecordBatch()) {
                    for (int i=0; i < root.getRowCount(); i++){
                        for (FieldVector fieldVector : root.getFieldVectors()) {
                            String fieldName = fieldVector.getField().getName();
                            String fieldContents = fieldVector.getObject(i).toString();
                            
                            // Check for Thread Safety Here 
                            LazoSketch sketch = hashes.get(fieldName);
                            sketch.update(fieldContents);
                            hashes.put(fieldName, sketch);
                        }
                    }
                    


                    rows += flightStream.getRoot().getRowCount();
                }
                
                /* 
                // Vector Unloader Based Approach 
                VectorUnloader unloader = new VectorUnloader(flightStream.getRoot());
                
                try (final ArrowRecordBatch arb = unloader.getRecordBatch()) {
                    batches.add(arb);
                    List<ArrowBuf> buffers = arb.getBuffers();
                    for (ArrowBuf buf : buffers) {
                        System.out.println("Buffer: "+ buf.toString());                    }
                    rows += flightStream.getRoot().getRowCount();
                }
                */
            }
            
            //Dataset dataset = new Dataset(batches, flightStream.getSchema(), rows);
            //datasets.put(flightStream.getDescriptor(), dataset);

            // Update the LazoIndex
            for (Field field : fields) {
                index.insert(field.getName(), hashes.get(field.getName()));
            }

            System.out.println("S1: Server: Indexed "+ rows +" rows");
            ackStream.onCompleted();

        };
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        FlightDescriptor flightDescriptor = FlightDescriptor.path(
                new String(ticket.getBytes(), StandardCharsets.UTF_8));
        Dataset dataset = this.datasets.get(flightDescriptor);
        if (dataset == null) {
            throw CallStatus.NOT_FOUND.withDescription("Unknown descriptor").toRuntimeException();
        } else {
            VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(
                    this.datasets.get(flightDescriptor).getSchema(), allocator);
            listener.start(vectorSchemaRoot);
            for (ArrowRecordBatch arrowRecordBatch : this.datasets.get(flightDescriptor).getBatches()) {
                VectorLoader loader = new VectorLoader(vectorSchemaRoot);
                loader.load(arrowRecordBatch.cloneWithTransfer(allocator));
                listener.putNext();
            }
            listener.completed();
        }
    }

    @Override
    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
        
        switch (action.getType()) {
            case "DELETE":
                FlightDescriptor flightDescriptor = FlightDescriptor.path(new String(action.getBody(), StandardCharsets.UTF_8));
                if (datasets.remove(flightDescriptor) != null) {
                    Result result = new Result("Delete completed".getBytes(StandardCharsets.UTF_8));
                    listener.onNext(result);
                } else {
                    Result result = new Result("Delete not completed. Reason: Key did not exist."
                            .getBytes(StandardCharsets.UTF_8));
                    listener.onNext(result);
                }
                listener.onCompleted();
            case "QUERY":
                LazoSketch sketch = new LazoSketch();
                try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(action.getBody()), allocator)) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    Schema schema = root.getSchema();
                    VectorUnloader unloader = new VectorUnloader(root);

                    // Unload the vector into a list of ArrowRecordBatch
                    try (final ArrowRecordBatch arb = unloader.getRecordBatch()) {
                        for (int i=0; i < root.getRowCount(); i++){
                            for (FieldVector fieldVector : root.getFieldVectors()) {
                                String fieldName = fieldVector.getField().getName();
                                String fieldContents = fieldVector.getObject(i).toString();
                                
                                // Check for Thread Safety Here 
                                // 
                                sketch.update(fieldContents);
                            }
                        }
                        
                    }
                
                Set<LazoCandidate> indexResult = index.queryContainment(sketch, jcx_threshold);
                Result result = new Result(indexResult.toString().getBytes(StandardCharsets.UTF_8));
                listener.onNext(result);
                  
                }
                catch (IOException e) {
                    Result result = new Result(("Query not completed. Reason: " + e.getMessage()).getBytes(StandardCharsets.UTF_8));
                    listener.onNext(result);
                }
        }
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        FlightEndpoint flightEndpoint = new FlightEndpoint(
                new Ticket(descriptor.getPath().get(0).getBytes(StandardCharsets.UTF_8)), location);
        return new FlightInfo(
                datasets.get(descriptor).getSchema(),
                descriptor,
                Collections.singletonList(flightEndpoint),
                /*bytes=*/-1,
                datasets.get(descriptor).getRows()
        );
    }

    @Override
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
        datasets.forEach((k, v) -> { listener.onNext(getFlightInfo(null, k)); });
        listener.onCompleted();
    }

   

    public static void main(String[] args) {
        Location location = Location.forGrpcInsecure("0.0.0.0", 33333);
        try (BufferAllocator allocator = new RootAllocator()){
            // Server
            try(FlightServer flightServer = FlightServer.builder(allocator, location,
                    new CookbookProducer(allocator, location)).build()) {
                try {
                    flightServer.start();
                    System.out.println("S1: Server (Location): Listening on port " + flightServer.getPort());
                    System.out.println("S1: Server (Location): Press CTRL-C to stop.");
                    flightServer.awaitTermination();

                } catch (IOException e) {
                    System.exit(1);
                }

            }
        } catch (InterruptedException e){
            e.printStackTrace();
        }
    }
}
