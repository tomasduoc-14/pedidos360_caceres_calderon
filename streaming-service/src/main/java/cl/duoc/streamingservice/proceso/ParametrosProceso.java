package cl.duoc.streamingservice.proceso;

/** Reintentos de los errores transitorios, con espera creciente. */
public record ParametrosProceso(int maxIntentos, long esperaInicialMs, long esperaMaximaMs) {
}
