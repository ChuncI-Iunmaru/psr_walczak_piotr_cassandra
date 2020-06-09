public class Goal {
    private String team;
    private String player;
    private int time;

    public Goal(String team, String player, int time) {
        this.team = team;
        this.player = player;
        this.time = time;
    }

    public String getTeam() {
        return team;
    }

    public String getPlayer() {
        return player;
    }

    public int getTime() {
        return time;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{")
                .append("team : '"+team+"'")
                .append(",")
                .append("time : "+time)
                .append(",")
                .append("player : '"+player+"'")
                .append("}");
        return builder.toString();
    }
}
