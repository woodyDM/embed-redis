package cn.deepmax.redis.args;

import io.netty.util.internal.StringUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * command line tool
 */
@Slf4j
public class Args {

    final Set<Flag<?>> parsed = new HashSet<>();

    /**
     * 解析
     *
     * @param args
     */
    public void parse(String[] args) {
        int pos = 0;
        int len = args.length;
        while (pos < len) {
            String idF = args[pos];
            Flag<?> flag = pick(idF);
            if (flag.parsed) {
                throw new IllegalArgumentException("id already parsed " + flag.id);
            }
            if (pos + 1 < len) {
                flag.fillValue(args[pos + 1]);
                pos += 2;
            } else {
                throw new IllegalArgumentException("invalid value for id " + idF);
            }
        }
        //to fill default
        parsed.stream().filter(f -> !f.parsed)
                .forEach(f -> f.fillValue(f.defaultValue));
        String text = parsed.stream().map(f -> f.id + "=" + f.data).collect(Collectors.joining(","));
        log.debug("Args: {}", text);
    }

    private Flag<?> pick(String idF) {
        boolean mdash;
        String id;
        if (idF.startsWith("--")) {
            mdash = true;
            id = idF.substring(2);
        } else if (idF.startsWith("-")) {
            mdash = false;
            id = idF.substring(1);
        } else {
            throw new IllegalArgumentException("invalid id [" + idF + "], should start with - or --");
        }
        if (StringUtil.isNullOrEmpty(id)) {
            throw new IllegalArgumentException("invalid id [" + idF + "]");
        }
        return parsed.stream().filter(f -> f.mdash == mdash && id.equals(f.id)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("invalid id [" + id + "] not found"));
    }

    public Args flag(Flag<?> f) {
        boolean old = parsed.add(f);
        if (!old) {
            throw new IllegalArgumentException("can't put more than once " + f.id);
        }
        return this;
    }

    public static class Flag<T> {
        private boolean parsed = false;
        private T data;
        private final String id;
        private final boolean mdash;  //单横线 双横线
        private final String defaultValue;
        private final Function<String, T> mapper;

        Flag(String id, boolean mdash, String defaultValue, Function<String, T> mapper) {
            this.id = id;
            this.mdash = mdash;
            this.defaultValue = defaultValue;
            this.mapper = mapper;
        }

        public T get() {
            return data;
        }

        public static <T> Flag<T> newInstance(String id, String defaultValue, Function<String, T> mapper) {
            if (StringUtil.isNullOrEmpty(id)) {
                throw new IllegalArgumentException("invalid id");
            }
            if (id.startsWith("-")) {
                throw new IllegalArgumentException("invalid id, should not start with -");
            }
            Objects.requireNonNull(mapper);
            return new Flag<>(id, id.length() > 1, defaultValue, mapper);
        }

        void fillValue(String v) {
            try {
                this.data = mapper.apply(v);

            } catch (Exception e) {
                throw new IllegalArgumentException("invalid value [" + v + "] for id " + id, e);
            }
            this.parsed = true;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Flag<?> flag = (Flag<?>) o;

            return id.equals(flag.id);
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }
    }
}
