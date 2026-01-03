package mangrobelabs.prometheus.util;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

// AI-generated class. Not reviewed.
public class ArrowDumper {
    public static <T> List<T> dumpTo(VectorSchemaRoot root, Class<T> clazz) throws Exception {
        List<T> result = new ArrayList<>();
        var fields = clazz.getDeclaredFields();
        int rowCount = root.getRowCount();

        for (int row = 0; row < rowCount; row++) {
            T obj = clazz.getDeclaredConstructor().newInstance();
            for (var f : fields) {
                f.setAccessible(true);
                var v = root.getVector(f.getName());
                if (v == null) {
                    continue;
                }
                Object value = readVectorValue(v, row, f.getType(), f.getGenericType());
                f.set(obj, value);
            }
            result.add(obj);
        }
        return result;
    }

    private static int getListStart(ListVector vector, int index) {
        return vector.getOffsetBuffer().getInt(index * 4);
    }

    private static Object readVectorValue(FieldVector vector, int index, Class<?> fieldType, Type genericType) throws Exception {
        if (vector.isNull(index)) {
            return null;
        }

        if (List.class.isAssignableFrom(fieldType)) {
            var elemType = getListElementType(genericType);
            if (vector instanceof ListVector) {
                return readList((ListVector) vector, index, elemType);
            }
            if (vector instanceof StructVector) {
                List<Object> singleton = new ArrayList<>(1);
                singleton.add(readStruct((StructVector) vector, index, elemType));
                return singleton;
            }
            throw new IllegalArgumentException("expected ListVector or StructVector for field type List, got: " + vector.getClass());
        }

        if (vector instanceof StructVector) {
            return readStruct((StructVector) vector, index, fieldType);
        }

        return coerceScalar(vector.getObject(index), fieldType);
    }

    private static List<?> readList(ListVector vector, int index, Class<?> elemType) throws Exception {
        int start = getListStart(vector, index);
        int end = getListStart(vector, index + 1);
        int size = Math.max(0, end - start);
        List<Object> out = new ArrayList<>(size);

        var data = vector.getDataVector();
        if (data instanceof StructVector) {
            var struct = (StructVector) data;
            for (int i = start; i < end; i++) {
                out.add(readStruct(struct, i, elemType));
            }
        } else {
            for (int i = start; i < end; i++) {
                out.add(coerceScalar(data.getObject(i), elemType));
            }
        }
        return out;
    }

    private static Object readStruct(StructVector vector, int index, Class<?> targetType) throws Exception {
        Object obj = targetType.getDeclaredConstructor().newInstance();
        var fields = targetType.getDeclaredFields();
        for (var f : fields) {
            f.setAccessible(true);
            var child = vector.getChild(f.getName());
            if (child == null) {
                continue;
            }
            Object value = readVectorValue(child, index, f.getType(), f.getGenericType());
            f.set(obj, value);
        }
        return obj;
    }

    private static Class<?> getListElementType(Type genericType) {
        if (genericType instanceof ParameterizedType) {
            var pt = (ParameterizedType) genericType;
            var args = pt.getActualTypeArguments();
            if (args.length == 1 && args[0] instanceof Class<?>) {
                return (Class<?>) args[0];
            }
        }
        return Object.class;
    }

    private static Object coerceScalar(Object value, Class<?> targetType) {
        if (value == null) {
            return null;
        }
        if (targetType.isInstance(value)) {
            return value;
        }
        if (targetType == String.class) {
            return value.toString();
        }
        if (value instanceof Number) {
            var n = (Number) value;
            if (targetType == long.class || targetType == Long.class) {
                return n.longValue();
            }
            if (targetType == int.class || targetType == Integer.class) {
                return n.intValue();
            }
            if (targetType == double.class || targetType == Double.class) {
                return n.doubleValue();
            }
            if (targetType == float.class || targetType == Float.class) {
                return n.floatValue();
            }
        }
        if (targetType == boolean.class || targetType == Boolean.class) {
            return Boolean.parseBoolean(value.toString());
        }
        return value;
    }
}
