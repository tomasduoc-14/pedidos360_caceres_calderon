package cl.duoc.streamingservice.proceso;

import static org.assertj.core.api.Assertions.assertThat;

import cl.duoc.streamingservice.error.ErrorIrrecuperableException;
import cl.duoc.streamingservice.error.ErrorTransitorioException;
import cl.duoc.streamingservice.dominio.ProcesadorEvento;
import cl.duoc.streamingservice.evento.EstadoPedidoEvento;
import cl.duoc.streamingservice.evento.EventoFallido;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

/** Prueba el recorrido de un evento sin KCL ni AWS: validacion, duplicados, reintentos y stream de fallidos. */
class ProcesadorEventosTest {

    private static final String JSON_VALIDO = """
            {"eventoId":"ev-1","tipo":"PEDIDO_CREADO","pedidoId":7,"producto":"Parlante","cantidad":2,
             "clienteEmail":"cliente@duoc.cl","estado":"PENDIENTE","ocurridoEn":"2026-11-12T12:00:00Z"}
            """;

    private final JsonMapper json = JsonMapper.builder().build();
    private final Validator validador = Validation.buildDefaultValidatorFactory().getValidator();
    private final Set<String> procesados = new HashSet<>();
    private final List<EventoFallido> fallidos = new ArrayList<>();
    private final List<Long> pausas = new ArrayList<>();
    private final Origen origen = new Origen("shardId-000000000000", "49600000000000000000000000000000000000000000000000000001", "7");

    private ProcesadorEventos<EstadoPedidoEvento> crear(ProcesadorEvento<EstadoPedidoEvento> negocio) {
        RegistroProcesados registro = new RegistroProcesados() {
            @Override
            public boolean yaProcesado(String aplicacion, String eventoId) {
                return procesados.contains(aplicacion + "#" + eventoId);
            }

            @Override
            public void marcar(String aplicacion, String eventoId) {
                procesados.add(aplicacion + "#" + eventoId);
            }
        };
        PublicadorFallidos publicador = fallido -> {
            fallidos.add(fallido);
            return true;
        };
        return new ProcesadorEventos<>("app-test", "stream-test", EstadoPedidoEvento.class,
                EstadoPedidoEvento::eventoId, negocio, json, validador, registro, publicador,
                new ParametrosProceso(3, 100, 400), ms -> pausas.add(ms));
    }

    @Test
    void eventoValidoSeProcesaYSeRegistra() {
        AtomicInteger llamadas = new AtomicInteger();
        var procesador = crear(e -> llamadas.incrementAndGet());

        assertThat(procesador.procesar(JSON_VALIDO, origen)).isEqualTo(ResultadoProceso.PROCESADO);
        assertThat(llamadas.get()).isEqualTo(1);
        assertThat(procesados).contains("app-test#ev-1");
        assertThat(fallidos).isEmpty();
    }

    @Test
    void eventoDuplicadoSeIgnora() {
        AtomicInteger llamadas = new AtomicInteger();
        var procesador = crear(e -> llamadas.incrementAndGet());

        procesador.procesar(JSON_VALIDO, origen);
        assertThat(procesador.procesar(JSON_VALIDO, origen)).isEqualTo(ResultadoProceso.DUPLICADO);
        assertThat(llamadas.get()).isEqualTo(1);
    }

    @Test
    void jsonInvalidoVaAlStreamDeFallidos() {
        var procesador = crear(e -> { });

        assertThat(procesador.procesar("esto no es json", origen)).isEqualTo(ResultadoProceso.ENVIADO_A_FALLIDOS);
        assertThat(fallidos).hasSize(1);
        assertThat(fallidos.get(0).causa()).startsWith("JSON invalido");
        assertThat(fallidos.get(0).payload()).isEqualTo("esto no es json");
        assertThat(fallidos.get(0).shardId()).isEqualTo("shardId-000000000000");
    }

    @Test
    void eventoConCamposFaltantesSeValidaYVaAFallidos() {
        AtomicInteger llamadas = new AtomicInteger();
        var procesador = crear(e -> llamadas.incrementAndGet());
        String sinProducto = JSON_VALIDO.replace("\"producto\":\"Parlante\"", "\"producto\":\"\"");

        assertThat(procesador.procesar(sinProducto, origen)).isEqualTo(ResultadoProceso.ENVIADO_A_FALLIDOS);
        assertThat(llamadas.get()).isZero();
        assertThat(fallidos.get(0).causa()).startsWith("evento invalido").contains("producto");
        assertThat(fallidos.get(0).eventoId()).isEqualTo("ev-1");
    }

    @Test
    void errorIrrecuperableNoSeReintenta() {
        AtomicInteger llamadas = new AtomicInteger();
        var procesador = crear(e -> {
            llamadas.incrementAndGet();
            throw new ErrorIrrecuperableException("dato invalido");
        });

        assertThat(procesador.procesar(JSON_VALIDO, origen)).isEqualTo(ResultadoProceso.ENVIADO_A_FALLIDOS);
        assertThat(llamadas.get()).isEqualTo(1);
        assertThat(pausas).isEmpty();
        assertThat(fallidos.get(0).causa()).isEqualTo("irrecuperable: dato invalido");
        assertThat(procesados).isEmpty();
    }

    @Test
    void errorTransitorioSeReintentaConEsperaCrecienteYLuegoFunciona() {
        AtomicInteger llamadas = new AtomicInteger();
        var procesador = crear(e -> {
            if (llamadas.incrementAndGet() <= 2) {
                throw new ErrorTransitorioException("servicio caido");
            }
        });

        assertThat(procesador.procesar(JSON_VALIDO, origen)).isEqualTo(ResultadoProceso.PROCESADO);
        assertThat(llamadas.get()).isEqualTo(3);
        assertThat(pausas).containsExactly(100L, 200L);
        assertThat(fallidos).isEmpty();
    }

    @Test
    void errorTransitorioPersistenteAgotaLosReintentosYVaAFallidos() {
        AtomicInteger llamadas = new AtomicInteger();
        var procesador = crear(e -> {
            llamadas.incrementAndGet();
            throw new ErrorTransitorioException("servicio caido");
        });

        assertThat(procesador.procesar(JSON_VALIDO, origen)).isEqualTo(ResultadoProceso.ENVIADO_A_FALLIDOS);
        assertThat(llamadas.get()).isEqualTo(3);
        assertThat(fallidos.get(0).intentos()).isEqualTo(3);
        assertThat(fallidos.get(0).causa()).isEqualTo("reintentos agotados: servicio caido");
    }
}
