package it.unitn.ds1;

public class MyTest {

    public static void main(String[] args) throws InterruptedException {
        while (true){
            System.out.println((int) (Math.random() * 10));
            Thread.sleep(1000);
        }
    }

}
