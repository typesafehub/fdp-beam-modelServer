package org.apache.beam.sdk.coders;

/**
 * Created by boris on 5/19/17.
 *
 * based on Beam's coders implementation in beam-sdks-java-core jar
 *
 */


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.util.PropertyNames;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeParameter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A {@link OptionalCoder} encodes OPtional values of type {@code T} using a nested
 * {@code Coder<T>} that defines whether the value is present. {@link OptionalCoder} uses
 * exactly 1 byte per entry to indicate whether the value is defined, then adds the encoding
 * of the inner coder for the actual values.
 *
 * @param <T> the type of the values being transcoded
 */
public class OptionalCoder <T> extends StandardCoder<Optional<T>> {

    public static <T> OptionalCoder<T> of(Coder<T> valueCoder) {
        return new OptionalCoder<>(valueCoder);
    }

    @JsonCreator
    public static OptionalCoder<?> of(
            @JsonProperty(PropertyNames.COMPONENT_ENCODINGS)
                    List<Coder<?>> components) {
        checkArgument(components.size() == 1, "Expecting 1 components, got %s", components.size());
        return of(components.get(0));
    }

    /////////////////////////////////////////////////////////////////////////////
    
    private final Coder<T> valueCoder;
    private static final int ENCODE_NOT_PRESENT = 0;
    private static final int ENCODE_PRESENT = 1;

    private OptionalCoder(Coder<T> valueCoder) {
        this.valueCoder = valueCoder;
    }

    /**
     * Returns the inner {@link Coder} wrapped by this {@link OptionalCoder} instance.
     */
    public Coder<T> getValueCoder() {
        return valueCoder;
    }

    @Override
    public void encode(Optional<T> value, OutputStream outStream, Context context)
            throws IOException, CoderException {
        if (value.isPresent()) {
            outStream.write(ENCODE_PRESENT);
            valueCoder.encode(value.get(), outStream, context);
        } else {
            outStream.write(ENCODE_NOT_PRESENT);
        }
    }

    @Override
    public Optional<T> decode(InputStream inStream, Context context) throws IOException, CoderException {
        int b = inStream.read();
        if (b == ENCODE_NOT_PRESENT) {
            return Optional.empty();
        } else if (b != ENCODE_PRESENT) {
            throw new CoderException(String.format(
                    "OptionalCoder expects either a byte valued %s (null) or %s (present), got %s",
                    ENCODE_NOT_PRESENT, ENCODE_PRESENT, b));
        }
        return Optional.of(valueCoder.decode(inStream, context));
    }

    @Override
    public List<Coder<T>> getCoderArguments() {
        return ImmutableList.of(valueCoder);
    }

    /**
     * {@code OptionalCoder} is deterministic if the nested {@code Coder} is.
     *
     * {@inheritDoc}
     */
    @Override
    public void verifyDeterministic() throws NonDeterministicException {
        verifyDeterministic("Value coder must be deterministic", valueCoder);
    }

    /**
     * {@code OptionalCoder} is consistent with equals if the nested {@code Coder} is.
     *
     * {@inheritDoc}
     */
    @Override
    public boolean consistentWithEquals() {
        return valueCoder.consistentWithEquals();
    }

    @Override
    public Object structuralValue(Optional<T> value) throws Exception {
        if (value.isPresent()) {
            return Optional.of(valueCoder.structuralValue(value.get()));
        }
        return com.google.common.base.Optional.absent();
    }

    /**
     * Overridden to short-circuit the default {@code StandardCoder} behavior of encoding and
     * counting the bytes. The size is known (1 byte) when {@code value} is absent, otherwise
     * the size is 1 byte plus the size of nested {@code Coder}'s encoding of {@code value}.
     *
     * {@inheritDoc}
     */
    @Override
    public void registerByteSizeObserver(
            Optional<T> value, ElementByteSizeObserver observer, Context context) throws Exception {
        observer.update(1);
        if (value.isPresent()) {
            valueCoder.registerByteSizeObserver(value.get(), observer, context);
        }
    }

    /**
     * Overridden to short-circuit the default {@code StandardCoder} behavior of encoding and
     * counting the bytes. The size is known (1 byte) when {@code value} is absent, otherwise
     * the size is 1 byte plus the size of nested {@code Coder}'s encoding of {@code value}.
     *
     * {@inheritDoc}
     */
    @Override
    protected long getEncodedElementByteSize(Optional<T> value, Context context) throws Exception {
        if (!value.isPresent()) {
            return 1;
        }

        if (valueCoder instanceof StandardCoder) {
            // If valueCoder is a StandardCoder then we can ask it directly for the encoded size of
            // the value, adding 1 byte to count the null indicator.
            return 1  + ((StandardCoder<T>) valueCoder)
                    .getEncodedElementByteSize(value.get(), context);
        }

        // If value is not a StandardCoder then fall back to the default StandardCoder behavior
        // of encoding and counting the bytes. The encoding will include the null indicator byte.
        return super.getEncodedElementByteSize(value, context);
    }

    /**
     * Returns the internal type.
     */
    public static <T> List<Object> getInstanceComponents(Optional<T> exampleValue) {
        if(exampleValue.isPresent())
            return Arrays.asList(exampleValue.get());
        return null;
    }

    /**
     * {@code OptionalCoder} is cheap if {@code valueCoder} is cheap.
     *
     * {@inheritDoc}
     */
    @Override
    public boolean isRegisterByteSizeObserverCheap(Optional<T> value, Context context) {
        if (!value.isPresent()) {
            return true;
        }
        return valueCoder.isRegisterByteSizeObserverCheap(value.get(), context);
    }

    @Override
    public TypeDescriptor<Optional<T>> getEncodedTypeDescriptor() {
        return new TypeDescriptor<Optional<T>>(getClass()) {}.where(
                new TypeParameter<T>() {}, getValueCoder().getEncodedTypeDescriptor());
    }
}
