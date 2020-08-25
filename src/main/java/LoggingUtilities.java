import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class LoggingUtilities {

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS %4$s [%2$s] %5$s%6$s%n");
    }

    private LoggingUtilities() {
    }

    public static void ensureLogLevel(Logger logger, final Level level) {

        for (;;) {
            setLogLevel(logger, level);
            if (!logger.getUseParentHandlers() || (logger = logger.getParent()) == null)
                break;
        }
    }

    private static void setLogLevel(final Logger logger, final Level level) {
        logger.setLevel(level);
        for (Handler h : logger.getHandlers())
            h.setLevel(level);
    }

}