package mykidong.annotation;

import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class ProcessAnnotation {

    @Test
    public void givenObjectSerializedThenTrueReturned() throws JsonSerializationException {

        Person person = new Person();
        person.setFirstName("soufiane");
        person.setLastName("cheouati");
        person.setAge(34);
        person.setAddress("any address...");

        ObjectToJsonConverter objectToJsonConverter = new ObjectToJsonConverter();
        String json = objectToJsonConverter.convertToJson(person);

        System.out.printf("json: %s\n", json);
    }

    private class ObjectToJsonConverter {
        public String convertToJson(Object object) throws JsonSerializationException {
            try {
                checkIfSerializable(object);
                initializeObject(object);
                return getJsonString(object);
            } catch (Exception e) {
                throw new JsonSerializationException(e.getMessage());
            }
        }
    }

    private void checkIfSerializable(Object object) {
        if (Objects.isNull(object)) {
            throw new JsonSerializationException("The object to serialize is null");
        }

        Class<?> clazz = object.getClass();
        if (!clazz.isAnnotationPresent(JsonSerializable.class)) {
            throw new JsonSerializationException("The class "
                    + clazz.getSimpleName()
                    + " is not annotated with JsonSerializable");
        }
    }

    private void initializeObject(Object object) throws Exception {
        Class<?> clazz = object.getClass();
        for (Method method : clazz.getDeclaredMethods()) {
            if (method.isAnnotationPresent(Init.class)) {
                method.setAccessible(true);
                method.invoke(object);
            }
        }
    }

    private String getJsonString(Object object) throws Exception {
        Class<?> clazz = object.getClass();
        Map<String, Object> jsonElementsMap = new HashMap<>();
        for (Field field : clazz.getDeclaredFields()) {
            field.setAccessible(true);
            if (field.isAnnotationPresent(JsonElement.class)) {

                JsonElement jsonElement = field.getAnnotation(JsonElement.class);
                String key = jsonElement.key();
                String fieldName = (key.equals("")) ? field.getName() : key;

                jsonElementsMap.put(fieldName, field.get(object));
            }
        }

        String jsonString = jsonElementsMap.entrySet()
                .stream()
                .map(entry -> {
                    String key = entry.getKey();
                    Object value = entry.getValue();

                    String valuePart = (value instanceof String) ? "\"" + value + "\""
                            : String.valueOf(value);

                    return "\"" + key + "\":" + valuePart;
                })
                .collect(Collectors.joining(","));
        return "{" + jsonString + "}";
    }
}
