import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.UdtValue;
import javafx.util.Pair;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;

public class ConsoleUtils {
    private static Scanner scanner = new Scanner(System.in);
    private static DateTimeFormatter format = DateTimeFormatter.ofPattern("dd-MM-yyyy");

    static String getFormattedDate(String setValue) {
        System.out.println("Podaj date meczu w formacie DD-MM-YYYY");
        if (!setValue.isEmpty())
            System.out.println("Obecna wartość: " + setValue + ". Pozostaw puste by nie zmieniać.");
        while (true) {
            try {
                String line = scanner.nextLine();
                if (!setValue.isEmpty() && line.isEmpty()) return setValue;
                LocalDate date = LocalDate.parse(line, format);
                return format.format(date);
            } catch (DateTimeParseException e) {
                System.out.println("Podaj prawidłową datę!");
            }
        }
    }

    static String getFormattedDate() {
        return getFormattedDate("");
    }

    static String getText(int minLength) {
        String tmp = "";
        do {
            tmp = scanner.nextLine();
            if (tmp.length() < minLength) System.out.println("Podaj minimum " + minLength + " znakow!");
        } while (tmp.length() < minLength);
        return tmp;
    }

    static int getNumber(int setValue, int minValue, int maxValue) {
        if (setValue != -1) System.out.println("Obecna wartość: " + setValue + ". Wpisz '-1' by nie zmieniać.");
        int num = minValue-1;
        while (num < minValue || (maxValue != -1 && num > maxValue)) {
            try {
                num = scanner.nextInt();
                scanner.nextLine();
                if (setValue != -1 && num == -1) return setValue;
                if (maxValue == -1 && num < minValue) System.out.println("Podaj prawidłową wartość >="+minValue);
                else if (maxValue != -1 && (num < minValue || num > maxValue))
                    System.out.println("Podaj prawidłową wartość z zakresu " + minValue + "-" + maxValue);
            } catch (InputMismatchException e) {
                scanner.next();
                System.out.println("Podaj prawidłową wartość!");
            }
        }
        return num;
    }

    static int getNumber(int minValue, int maxValue) {
        return getNumber(-1, minValue,  maxValue);
    }

    static int getNumber(int setValue) {
        return getNumber(setValue, 1, -1);
    }

    static int getNumber() {
        return getNumber(-1, 1, -1);
    }

    static Pair<Integer, Integer> getScores(String firstTeam, String secondTeam, Pair<Integer, Integer> setValue) {
        int firstScore, secondScore;
        System.out.println("Podaj punkty dla drużyny: " + firstTeam);
        firstScore = getNumber(setValue.getKey(), 0, -1);
        System.out.println("Podaj punkty dla drużyny: " + secondTeam);
        secondScore = getNumber(setValue.getValue(), 0, -1);
        return new Pair<>(firstScore, secondScore);
    }

    static Pair<Integer, Integer> getScores(String firstTeam, String secondTeam) {
        return getScores(firstTeam, secondTeam, new Pair<>(-1, -1));
    }

    static String pickTeam(String firstTeam, String secondTeam){
        System.out.println("Wybierz drużynę: \n1."+firstTeam+"\n2."+secondTeam);
        int choice = getNumber(1,2);
        return choice == 1 ? firstTeam : secondTeam;
    }

    static void printGoal(Goal goal){

    }

    static List<Goal> getGoalsForTeam(int number, int matchTime, String team) {
        if (number == 0) return Collections.emptyList();
        List<Goal> goals = new ArrayList<>();
        int tmpTime;
        String tmpPlayer;
        for (int i = 1; i <= number; i++) {
            System.out.println("Podaj dane " + i + "-go gola dla drużyny " + team);
            System.out.println("Podaj czas zdobycia gola: ");
            tmpTime = ConsoleUtils.getNumber(1, matchTime);
            System.out.println("Podaj nazwisko zawodnika który zdobył gola: ");
            tmpPlayer = ConsoleUtils.getText(1);
            goals.add(new Goal(team, tmpPlayer, tmpTime));
        }
        return goals;
    }

    static void printRowAsMatch(Row row) {
        //75 znaków?
        System.out.println("Id: "+row.getInt("id"));
        System.out.format("%-15s na %-30s\n", row.getString("date"), row.getString("stadium"));
        System.out.format("%30s vs. %-30s\n", row.getString("team1"), row.getString("team2"));
        Pair<Integer, Integer> score = new Pair<>(row.getInt("score1"), row.getInt("score2"));
        System.out.format("%30s: - :%-30s\n", score.getKey(), score.getValue());
        if (score.getKey().equals(score.getValue())) {
            System.out.format("%40s\n", "REMIS");
        } else if (score.getKey() > score.getValue()) {
            System.out.format("%-40s\n", "Zwycięzca: " + row.getString("team1"));
        } else System.out.format("%75s\n", "Zwycięzca: " + row.getString("team2"));
        System.out.format(String.format("%75s\n", "-").replace(' ', '-'));
        List<UdtValue> goals = row.getList("goals", UdtValue.class);
        assert goals != null;
        for (UdtValue g : goals) {
            System.out.format("%20s w: %3d minucie. Zdobył(a): %-30s\n", g.getString("team"), g.getInt("time"), g.getString("player"));
        }
        System.out.println();
    }
}
