package cl.duoc.streamingservice.kcl;

import cl.duoc.streamingservice.config.StreamingProperties;
import cl.duoc.streamingservice.dominio.AnaliticaPedidosService;
import cl.duoc.streamingservice.dominio.NotificacionService;
import cl.duoc.streamingservice.dominio.ProcesadorEvento;
import cl.duoc.streamingservice.dominio.TendenciasService;
import cl.duoc.streamingservice.evento.ActividadClienteEvento;
import cl.duoc.streamingservice.evento.EstadoPedidoEvento;
import cl.duoc.streamingservice.proceso.ParametrosProceso;
import cl.duoc.streamingservice.proceso.ProcesadorEventos;
import cl.duoc.streamingservice.proceso.PublicadorFallidos;
import cl.duoc.streamingservice.proceso.RegistroProcesados;
import jakarta.validation.Validator;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import tools.jackson.databind.json.JsonMapper;

/** Arma las aplicaciones consumidoras: une cada flujo de negocio con su stream y su nombre de aplicacion. */
@Component
@ConditionalOnProperty(name = "streaming.habilitado", havingValue = "true")
public class FabricaConsumidores {

    private final StreamingProperties props;
    private final JsonMapper json;
    private final Validator validador;
    private final RegistroProcesados procesados;
    private final PublicadorFallidos fallidos;
    private final KinesisAsyncClient kinesis;
    private final DynamoDbAsyncClient dynamo;
    private final CloudWatchAsyncClient cloudWatch;
    private final NotificacionService notificaciones;
    private final AnaliticaPedidosService analitica;
    private final TendenciasService tendencias;

    public FabricaConsumidores(StreamingProperties props, JsonMapper json, Validator validador,
                               RegistroProcesados procesados, PublicadorFallidos fallidos,
                               KinesisAsyncClient kinesis, DynamoDbAsyncClient dynamo,
                               CloudWatchAsyncClient cloudWatch, NotificacionService notificaciones,
                               AnaliticaPedidosService analitica, TendenciasService tendencias) {
        this.props = props;
        this.json = json;
        this.validador = validador;
        this.procesados = procesados;
        this.fallidos = fallidos;
        this.kinesis = kinesis;
        this.dynamo = dynamo;
        this.cloudWatch = cloudWatch;
        this.notificaciones = notificaciones;
        this.analitica = analitica;
        this.tendencias = tendencias;
    }

    public List<AplicacionConsumidora> crear() {
        StreamingProperties.Consumidores c = props.consumidores();
        List<AplicacionConsumidora> aplicaciones = new ArrayList<>();
        if (c.notificaciones().habilitada()) {
            aplicaciones.add(aplicacion(c.notificaciones(), EstadoPedidoEvento.class,
                    EstadoPedidoEvento::eventoId, notificaciones));
        }
        if (c.analitica().habilitada()) {
            aplicaciones.add(aplicacion(c.analitica(), EstadoPedidoEvento.class,
                    EstadoPedidoEvento::eventoId, analitica));
        }
        if (c.tendencias().habilitada()) {
            aplicaciones.add(aplicacion(c.tendencias(), ActividadClienteEvento.class,
                    ActividadClienteEvento::eventoId, tendencias));
        }
        return aplicaciones;
    }

    private <T> AplicacionConsumidora aplicacion(StreamingProperties.App cfg, Class<T> tipo,
                                                 Function<T, String> idDe, ProcesadorEvento<T> negocio) {
        StreamingProperties.Procesamiento p = props.procesamiento();
        ProcesadorEventos<T> procesador = new ProcesadorEventos<>(cfg.nombre(), cfg.stream(), tipo, idDe,
                negocio, json, validador, procesados, fallidos,
                new ParametrosProceso(p.maxIntentos(), p.esperaInicialMs(), p.esperaMaximaMs()),
                ProcesadorEventos::dormir);
        ShardRecordProcessorFactory fabrica = () -> new ProcesadorShard<>(cfg.nombre(), procesador);
        return new AplicacionConsumidora(cfg.nombre(), cfg.stream(), fabrica, kinesis, dynamo, cloudWatch, props.kcl());
    }
}
