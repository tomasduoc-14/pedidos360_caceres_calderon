package cl.duoc.pedidosservice.service;

import cl.duoc.pedidosservice.streaming.evento.ActividadClienteEvento;
import cl.duoc.pedidosservice.streaming.evento.TipoActividad;
import cl.duoc.pedidosservice.streaming.productor.ActividadStreamPublicador;
import org.springframework.stereotype.Service;

/** Registra la actividad de un cliente como evento. No conoce Kinesis, solo la interfaz publicadora. */
@Service
public class ActividadService {

    private final ActividadStreamPublicador publicador;

    public ActividadService(ActividadStreamPublicador publicador) {
        this.publicador = publicador;
    }

    /** Devuelve el eventoId asignado al evento. */
    public String registrar(TipoActividad tipo, String clienteEmail, String producto) {
        ActividadClienteEvento evento = ActividadClienteEvento.de(tipo, clienteEmail, producto);
        publicador.publicar(evento);
        return evento.eventoId();
    }
}
