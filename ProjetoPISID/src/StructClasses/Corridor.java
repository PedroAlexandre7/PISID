package StructClasses;

public record Corridor(int from, int to) {

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Corridor corridor = (Corridor) o;
        return from == corridor.from && to == corridor.to;
    }
}


