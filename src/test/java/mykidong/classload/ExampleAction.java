package mykidong.classload;

public class ExampleAction implements Action {

    @Override
    public void run() {
        System.out.println("it is invoked by reflection!!!");
    }
}
