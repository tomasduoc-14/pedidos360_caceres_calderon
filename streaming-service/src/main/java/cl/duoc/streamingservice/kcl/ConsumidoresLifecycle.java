package cl.duoc.streamingservice.kcl;

import java.util.List;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/** Arranca las aplicaciones consumidoras cuando la aplicacion esta lista y las detiene de forma ordenada. */
@Component
@ConditionalOnProperty(name = "streaming.habilitado", havingValue = "true")
public class ConsumidoresLifecycle implements SmartLifecycle {

    private final List<AplicacionConsumidora> aplicaciones;
    private volatile boolean activo;

    public ConsumidoresLifecycle(FabricaConsumidores fabrica) {
        this.aplicaciones = fabrica.crear();
    }

    @Override
    public void start() {
        aplicaciones.forEach(AplicacionConsumidora::iniciar);
        activo = true;
    }

    @Override
    public void stop() {
        aplicaciones.forEach(AplicacionConsumidora::detener);
        activo = false;
    }

    @Override
    public boolean isRunning() {
        return activo;
    }
}
