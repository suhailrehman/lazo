package lazo.arrowserver.flightrpc;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

class CookbookProducer extends NoOpFlightProducer {
    private final BufferAllocator allocator;
    private final Location location;
    private final ConcurrentHashMap<FlightDescriptor, Dataset> datasets;
    public CookbookProducer(BufferAllocator allocator, Location location) {
        this.allocator = allocator;
        this.location = location;
        this.datasets = new ConcurrentHashMap<>();
    }
    @Override
    public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        List<ArrowRecordBatch> batches = new ArrayList<>();
        return () -> {
            long rows = 0;
            VectorUnloader unloader;
            while (flightStream.next()) {
                unloader = new VectorUnloader(flightStream.getRoot());
                try (final ArrowRecordBatch arb = unloader.getRecordBatch()) {
                    batches.add(arb);
                    rows += flightStream.getRoot().getRowCount();
                }
            }
            Dataset dataset = new Dataset(batches, flightStream.getSchema(), rows);
            datasets.put(flightStream.getDescriptor(), dataset);
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
        FlightDescriptor flightDescriptor = FlightDescriptor.path(
                new String(action.getBody(), StandardCharsets.UTF_8));
        switch (action.getType()) {
            case "DELETE":
                if (datasets.remove(flightDescriptor) != null) {
                    Result result = new Result("Delete completed".getBytes(StandardCharsets.UTF_8));
                    listener.onNext(result);
                } else {
                    Result result = new Result("Delete not completed. Reason: Key did not exist."
                            .getBytes(StandardCharsets.UTF_8));
                    listener.onNext(result);
                }
                listener.onCompleted();
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
}