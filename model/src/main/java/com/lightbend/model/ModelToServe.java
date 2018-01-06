package com.lightbend.model;

import com.lightbend.model.PMML.PMMLModelFactory;
import com.lightbend.model.tensorflow.TensorflowModelFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

// Intermediate model representation used for transporting models
public class ModelToServe implements Serializable {

    private static final Map<Integer, ModelFactory> factories = new HashMap<Integer, ModelFactory>() {
        {
            put(Modeldescriptor.ModelDescriptor.ModelType.TENSORFLOW.getNumber(), TensorflowModelFactory.getInstance());
            put(Modeldescriptor.ModelDescriptor.ModelType.PMML.getNumber(), PMMLModelFactory.getInstance());
        }
    };

    private String name;
    private String description;
    private Modeldescriptor.ModelDescriptor.ModelType modelType;
    private byte[] modelData;
    private String modelDataLocation;
    private String dataType;

    public ModelToServe(String name, String description, Modeldescriptor.ModelDescriptor.ModelType modelType,
                        byte[] dataContent, String modelDataLocation, String dataType){
        this.name = name;
        this.description = description;
        this.modelType = modelType;
        this.modelData = dataContent;
        this.modelDataLocation = modelDataLocation;
        this.dataType = dataType;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public Modeldescriptor.ModelDescriptor.ModelType getModelType() {
        return modelType;
    }

    public String getDataType() {
        return dataType;
    }

    public byte[] getModelData() {
        return modelData;
    }

    public String getModelDataLocation() {
        return modelDataLocation;
    }

    @Override
    public String toString() {
        return "ModelToServe{" +
                "name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", modelType=" + modelType +
                ", dataType='" + dataType + '\'' +
                '}';
    }

    public static Model restore(int type, byte[] bytes){
        ModelFactory factory = factories.get(type);
        if (factory == null) {
            System.out.println("Unknown model type " + type);
            return null;
        }
        return factory.restore(bytes);
    }

    public static Optional<Model> create(ModelToServe descriptor){
        ModelFactory factory = factories.get(descriptor.getModelType().ordinal());
        if (factory == null) {
            System.out.println("Unknown model type " + descriptor.getModelType());
            return Optional.empty();
        }
        return factory.create(descriptor);
    }
}
