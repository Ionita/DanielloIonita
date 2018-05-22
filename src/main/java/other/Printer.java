package other;

/**
 * Author : Simone D'Aniello
 * Date :  25/02/2018.
 */
public class Printer {

    private static Printer instance = new Printer();

    private Printer(){}

    public static Printer getInstance(){return instance;}

    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_BLACK = "\u001B[30m";
    private static final String ANSI_RED = "\u001B[31m";
    private static final String ANSI_GREEN = "\u001B[32m";
    private static final String ANSI_YELLOW = "\u001B[33m";
    private static final String ANSI_BLUE = "\u001B[34m";
    private static final String ANSI_PURPLE = "\u001B[35m";
    private static final String ANSI_CYAN = "\u001B[36m";

    public void print(String text, String color) {
        switch (color) {
            case "blue":
                System.out.println(ANSI_BLUE + text + ANSI_RESET);
                break;
            case "black":
                System.out.println(ANSI_BLACK + text + ANSI_RESET);
                break;
            case "red":
                System.out.println(ANSI_RED + text + ANSI_RESET);
                break;
            case "green":
                System.out.println(ANSI_GREEN + text + ANSI_RESET);
                break;
            case "yellow":
                System.out.println(ANSI_YELLOW + text + ANSI_RESET);
                break;
            case "purple":
                System.out.println(ANSI_PURPLE + text + ANSI_RESET);
                break;
            case "cyan":
                System.out.println(ANSI_CYAN + text + ANSI_RESET);
                break;
            default:
                System.out.println("color not supported. \nInsert one of the following:");
                print("blue", "blue");
                print("black", "black");
                print("red", "red");
                print("green", "green");
                print("yellow", "yellow");
                print("purple", "purple");
                print("cyan", "cyan");
                break;
        }

    }


}
