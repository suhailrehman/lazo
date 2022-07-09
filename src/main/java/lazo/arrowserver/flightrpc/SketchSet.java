package lazo.arrowserver.flightrpc;

import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.arrow.vector.types.pojo.Schema;
import lazo.sketch.LazoSketch;


class SketchSet implements Serializable{
    private transient ConcurrentHashMap<String,LazoSketch> sketch;
    private transient Schema schema;

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

    private void writeObject (java.io.ObjectOutputStream out) throws java.io.IOException {
        out.writeObject(sketch);
        out.writeObject(schema.toJson());
    }

    @SuppressWarnings("unchecked")
    private void readObject (java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
        sketch = (ConcurrentHashMap<String, LazoSketch>) in.readObject();
        schema = Schema.fromJSON((String) in.readObject());
    }
}
