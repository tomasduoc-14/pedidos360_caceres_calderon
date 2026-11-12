package cl.duoc.streamingservice.proceso;

import cl.duoc.streamingservice.dominio.ProcesadorEvento;
import cl.duoc.streamingservice.error.ErrorIrrecuperableException;
import cl.duoc.streamingservice.evento.EventoFallido;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import java.time.Instant;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.json.JsonMapper;

/**
 * Recorrido de un evento por una aplicacion consumidora:
 * JSON -> validacion -> duplicado? -> negocio con reintentos -> (fallidos | procesado).
 *
 * Nunca lanza excepciones: un evento defectuoso termina en el stream de fallidos y el shard sigue.
 * Todos los logs llevan el id del evento, el intento y la causa.
 */
public final class ProcesadorEventos<T> {

    private static final Logger log = LoggerFactory.getLogger(ProcesadorEventos.class);

    private final String aplicacion;
    private final String stream;
    private final Class<T> tipo;
    private final Function<T, String> idDe;
    private final ProcesadorEvento<T> negocio;
    private final JsonMapper json;
    private final Validator validador;
    private final RegistroProcesados procesados;
    private final PublicadorFallidos fallidos;
    private final ParametrosProceso parametros;
    private final LongConsumer pausa;

    public ProcesadorEventos(String aplicacion, String stream, Class<T> tipo, Function<T, String> idDe,
                             ProcesadorEvento<T> negocio, JsonMapper json, Validator validador,
                             RegistroProcesados procesados, PublicadorFallidos fallidos,
                             ParametrosProceso parametros, LongConsumer pausa) {
        this.aplicacion = aplicacion;
        this.stream = stream;
        this.tipo = tipo;
        this.idDe = idDe;
        this.negocio = negocio;
        this.json = json;
        this.validador = validador;
        this.procesados = procesados;
        this.fallidos = fallidos;
        this.parametros = parametros;
        this.pausa = pausa;
    }

    public static void dormir(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public ResultadoProceso procesar(String texto, Origen origen) {
        // 1. Deserializar
        T evento;
        try {
            evento = json.readValue(texto, tipo);
        } catch (RuntimeException e) {
            return aFallidos("desconocido", texto, origen, 1, "JSON invalido: " + e.getMessage());
        }
        if (evento == null) {
            return aFallidos("desconocido", texto, origen, 1, "JSON invalido: evento vacio");
        }
        String id = Objects.toString(idDe.apply(evento), "desconocido");

        // 2. Validar el contenido
        Set<ConstraintViolation<T>> errores = validador.validate(evento);
        if (!errores.isEmpty()) {
            String causa = errores.stream()
                    .map(v -> v.getPropertyPath() + " " + v.getMessage())
                    .sorted()
                    .collect(Collectors.joining("; "));
            return aFallidos(id, texto, origen, 1, "evento invalido: " + causa);
        }

        // 3. Duplicado: lo ignoramos
        if (yaProcesado(id)) {
            log.info("DUPLICADO app={} eventoId={} shard={} sequenceNumber={}",
                    aplicacion, id, origen.shardId(), origen.sequenceNumber());
            return ResultadoProceso.DUPLICADO;
        }

        // 4. Negocio con reintentos de los errores transitorios
        long espera = parametros.esperaInicialMs();
        String ultimaCausa = "sin detalle";
        for (int intento = 1; intento <= parametros.maxIntentos(); intento++) {
            try {
                negocio.procesar(evento);
                marcar(id);
                log.info("PROCESADO app={} eventoId={} shard={} sequenceNumber={} intento={}/{}",
                        aplicacion, id, origen.shardId(), origen.sequenceNumber(), intento, parametros.maxIntentos());
                return ResultadoProceso.PROCESADO;
            } catch (ErrorIrrecuperableException e) {
                return aFallidos(id, texto, origen, intento, "irrecuperable: " + e.getMessage());
            } catch (RuntimeException e) {
                // ErrorTransitorioException y cualquier error inesperado se tratan como transitorios.
                ultimaCausa = String.valueOf(e.getMessage());
                log.warn("REINTENTO app={} eventoId={} intento={}/{} causa={}",
                        aplicacion, id, intento, parametros.maxIntentos(), ultimaCausa);
                if (intento < parametros.maxIntentos()) {
                    pausa.accept(espera);
                    espera = Math.min(espera * 2, parametros.esperaMaximaMs());
                }
            }
        }
        return aFallidos(id, texto, origen, parametros.maxIntentos(), "reintentos agotados: " + ultimaCausa);
    }

    private boolean yaProcesado(String id) {
        try {
            return procesados.yaProcesado(aplicacion, id);
        } catch (RuntimeException e) {
            // Si no podemos consultar, preferimos procesar de mas (al menos una vez) que perder el evento.
            log.warn("No se pudo consultar duplicados app={} eventoId={} causa={}", aplicacion, id, e.getMessage());
            return false;
        }
    }

    private void marcar(String id) {
        try {
            procesados.marcar(aplicacion, id);
        } catch (RuntimeException e) {
            log.warn("No se pudo registrar el eventoId como procesado app={} eventoId={} causa={}",
                    aplicacion, id, e.getMessage());
        }
    }

    private ResultadoProceso aFallidos(String id, String texto, Origen origen, int intentos, String causa) {
        log.error("A FALLIDOS app={} eventoId={} shard={} sequenceNumber={} intentos={} causa={}",
                aplicacion, id, origen.shardId(), origen.sequenceNumber(), intentos, causa);
        EventoFallido fallido = new EventoFallido(id, aplicacion, stream, origen.shardId(),
                origen.sequenceNumber(), causa, intentos, texto, Instant.now().toString());
        boolean enviado;
        try {
            enviado = fallidos.enviar(fallido);
        } catch (RuntimeException e) {
            enviado = false;
        }
        if (!enviado) {
            log.error("PERDIDO app={} eventoId={} no se pudo escribir en el stream de fallidos; payload={}",
                    aplicacion, id, texto);
            return ResultadoProceso.PERDIDO;
        }
        return ResultadoProceso.ENVIADO_A_FALLIDOS;
    }
}
