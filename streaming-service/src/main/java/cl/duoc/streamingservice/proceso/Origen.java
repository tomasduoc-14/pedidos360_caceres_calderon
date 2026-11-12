package cl.duoc.streamingservice.proceso;

/** De donde vino el registro: sirve para los logs y para el stream de fallidos. */
public record Origen(String shardId, String sequenceNumber, String claveParticion) {
}
