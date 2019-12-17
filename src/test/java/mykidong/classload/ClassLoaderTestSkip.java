package mykidong.classload;

import mykidong.util.ClassLoaderUtils;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

public class ClassLoaderTestSkip {

    @Test
    public void loadExternalClassToClassLoader() throws Exception
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        System.out.println("classloader: " + classLoader);
        System.out.println("classloader parent: " + classLoader.getParent());

        String classFullName = "mykidong.classload.ExampleExternalBean";

        // Classpath 내부에 있지 않기 때문에 Error.
        try {
            final Class<?> externalFromUrl = classLoader.loadClass(classFullName);
        } catch (Exception e)
        {
            e.printStackTrace();
            System.err.println("Classpath 에 있지 않기 때문에 당연히 Error 발생!!!");
        }


        // 외부에 있는 Class 를 ClassLoader 에 Loading.
        String externalPath = "C:/tmp/external/";
        File externalClassFilePath = new File(externalPath);
        try {
            final URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{externalClassFilePath.toURI().toURL()});
            Thread.currentThread().setContextClassLoader(urlClassLoader);
            classLoader = Thread.currentThread().getContextClassLoader();

            System.out.println("with url classloader: classloader: " + classLoader);
            System.out.println("with url classloader: classloader parent: " + classLoader.getParent());

            final Class<?> externalClazz = classLoader.loadClass(classFullName);

            // print instance methods.
            for (Method method : externalClazz.getDeclaredMethods()) {
                System.out.println("method: [" + method.getName() + "]");
            }


            System.out.println("===================== print all classes loaded into the classloader.");

            // print all classes loaded into the classloader.
            ClassLoaderUtils.printAllClasses(classLoader);
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    @Test
    public void runCustomClassLoader() throws Exception
    {
        // classpath 내의 class 를 loading.

        String classFullName = "mykidong.util.ClassLoaderUtils";

        ClassLoader classLoader = new CustomClassLoader();
        final Class<?> clazz = classLoader.loadClass(classFullName);

        // print instance methods.
        for (Method method : clazz.getDeclaredMethods()) {
            System.out.println("method: [" + method.getName() + "]");
        }
    }

    @Test
    public void callMethodByReflection() throws Exception
    {
        // classpath 내의 class 를 loading.

        String classFullName = "mykidong.classload.ExampleAction";

        ClassLoader classLoader = new CustomClassLoader();
        final Class<?> clazz = classLoader.loadClass(classFullName);
        final Object obj = clazz.getConstructor().newInstance();

        // with instance casting.
        if(obj instanceof Action)
        {
            Action action = (Action) obj;
            action.run();
        }
    }

    @Test
    public void callMethodByReflection2() throws Exception {
        // classpath 내의 class 를 loading.

        String classFullName = "mykidong.classload.ExampleAction";

        ClassLoader classLoader = new CustomClassLoader();
        final Class<?> clazz = classLoader.loadClass(classFullName);
        final Object obj = clazz.getConstructor().newInstance();


        // with method invokation.
        for (Method method : clazz.getDeclaredMethods()) {
            if (method.getName().equals("run")) {
                method.invoke(obj);
            }
        }
    }
}
