"""Pruebas de regresión para las mejoras de resiliencia documentadas."""

from src.recuperacion.plan_drp import PlanDRP
from src.resiliencia.particionamiento import GestorParticionamiento
from src.resiliencia.replicacion import EstadoNodo, NodoReplicacion, RolNodo


def test_latencia_simulada_no_puede_ser_negativa(monkeypatch):
    """Una perturbación negativa no debe generar una latencia imposible."""
    monkeypatch.setattr("src.resiliencia.replicacion.random.uniform", lambda _a, _b: -2)
    monkeypatch.setattr("src.resiliencia.replicacion.time.sleep", lambda _s: None)
    nodo = NodoReplicacion("nodo-prueba", RolNodo.LIDER, latencia_red_ms=1.0)

    exito, latencia_ms = nodo.escribir("tx-1", {"monto": 10})

    assert exito is True
    assert latencia_ms == 0.0
    assert nodo.leer("tx-1") == {"monto": 10}
    assert nodo.estado is EstadoNodo.ACTIVO


def test_sharding_hash_distribuye_lote_en_los_tres_shards():
    """Una clave de cliente de alta cardinalidad utiliza todos los shards."""
    gestor = GestorParticionamiento(num_shards=3, estrategia="hash")
    lote = [
        {"id_transaccion": f"tx-{i}", "cliente_id": f"cliente-{i:04d}"}
        for i in range(500)
    ]

    distribucion = gestor.insertar_lote(lote)

    assert sum(distribucion.values()) == len(lote)
    assert set(distribucion) == {"shard_0", "shard_1", "shard_2"}
    assert all(cantidad > 0 for cantidad in distribucion.values())


def test_drp_copia_todos_los_registros_pendientes_sin_duplicarlos(monkeypatch):
    """La sincronización incremental copia el lote inicial completo y luego solo nuevos datos."""
    drp = PlanDRP()

    def sincronizar_sin_espera(datos):
        drp.sitio_standby.datos_sincronizados.extend(datos)
        return True

    monkeypatch.setattr(drp.sitio_standby, "sincronizar", sincronizar_sin_espera)
    for numero in range(200):
        drp.registrar_dato({"id_transaccion": f"tx-{numero}"})

    copiados_iniciales = drp._replicar_pendientes()
    copiados_duplicados = drp._replicar_pendientes()
    drp.registrar_dato({"id_transaccion": "tx-200"})
    copiados_incrementales = drp._replicar_pendientes()

    assert copiados_iniciales == 200
    assert copiados_duplicados == 0
    assert copiados_incrementales == 1
    assert len(drp.sitio_standby.datos_sincronizados) == 201
