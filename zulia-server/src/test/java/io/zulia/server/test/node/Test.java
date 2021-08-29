package io.zulia.server.test.node;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.DDSketchProtoBinding;
import com.datadoghq.sketch.ddsketch.DDSketches;

import java.util.stream.DoubleStream;

public class Test {

	public static void main(String[] args) {
		double relativeAccuracy = 0.001;
		DDSketch sketch = DDSketches.unboundedDense(relativeAccuracy);

		// Adding values to the sketch
		sketch.accept(3.2); // adds a single value
		sketch.accept(2.1, 3); // adds multiple times the same value

		// Querying the sketch
		sketch.getValueAtQuantile(0.5); // returns the median value
		sketch.getMinValue();
		sketch.getMaxValue();

		// Merging another sketch into the sketch, in-place
		DDSketch anotherSketch = DDSketches.unboundedDense(relativeAccuracy);
		DoubleStream.of(3.4, 7.6, 2.8).forEach(anotherSketch);
		sketch.mergeWith(anotherSketch);

		com.datadoghq.sketch.ddsketch.proto.DDSketch ddSketch = DDSketchProtoBinding.toProto(sketch);

		System.out.println(sketch.getMaxValue());
		System.out.println(sketch.getMinValue());
	}
}
