package lazo.arrowserver.flightrpc;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.arrow.vector.types.pojo.Schema;
import lazo.sketch.LazoSketch;


class SketchSet {
    private final ConcurrentHashMap<String,LazoSketch> sketch;
    private final Schema schema;

    public SketchSet(Schema schema) {
        this.sketch = new ConcurrentHashMap<>();
        this.schema = schema;
    }

    public void setSketch(String key, LazoSketch sketch) {
        this.sketch.put(key, sketch);
    }

    public LazoSketch getSketch(String key) {
        return sketch.get(key);
    }

    public Collection<LazoSketch> getAllSketches() {
        return sketch.values();
    }

    public Schema getSchema() {
        return schema;
    }
}
