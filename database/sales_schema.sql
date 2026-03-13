-- Drop previous limited table
DROP TABLE IF EXISTS public.ventas_detalle;

-- Table to store monthly sales data matching exact Excel structure
CREATE TABLE public.ventas_detalle (
    id BIGSERIAL PRIMARY KEY,
    idunica TEXT, -- Match "IDUNICA" from Excel
    cliente INT, -- "Cliente"
    direccion TEXT, -- "Direccion"
    fecha DATE, -- "Fecha"
    comprobante TEXT, -- "Comprobante"
    art INT, -- "Art"
    cantidad NUMERIC, -- "Cantidad"
    importe NUMERIC, -- "Importe"
    razon_social TEXT, -- "Razon_Social"
    motivodev TEXT, -- "motivodev"
    descuento NUMERIC, -- "descuento"
    cod_ven INT, -- "cod_ven"
    articulo TEXT, -- "articulo"
    neto NUMERIC, -- "neto"
    camion TEXT, -- "camion"
    comentario TEXT, -- "comentario"
    subcanal TEXT, -- "subcanal"
    reparto INT, -- "reparto"
    pr_costo_uni_neto NUMERIC, -- "pr_costo_uni_neto"
    chofer TEXT, -- "chofer"
    valordesc NUMERIC, -- "valordesc"
    facturacion NUMERIC, -- "facturacion"
    cmv NUMERIC, -- "CMV"
    d1 NUMERIC, -- "d1"
    d2 NUMERIC, -- "d2"
    peso NUMERIC, -- "peso"
    rubro TEXT, -- "rubro"
    descripcion TEXT, -- "descripcion"
    capacidad_art NUMERIC, -- "capacidad_art"
    usuariopicking TEXT, -- "usuariopicking"
    nombrepicking TEXT, -- "nombrepicking"
    tipov TEXT, -- "tipoV"
    segmentoproducto TEXT, -- "segmentoproducto"
    linea TEXT, -- "linea"
    fecha_pedido TIMESTAMP WITHOUT TIME ZONE, -- "fecha_pedido"
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB DEFAULT '{}'::jsonb,
    CONSTRAINT unique_comprobante_articulo UNIQUE (comprobante, articulo)
);

-- Indices for reporting and agent analysis
CREATE INDEX IF NOT EXISTS idx_ventas_fecha ON public.ventas_detalle(fecha);
CREATE INDEX IF NOT EXISTS idx_ventas_cliente ON public.ventas_detalle(cliente);
CREATE INDEX IF NOT EXISTS idx_ventas_art ON public.ventas_detalle(art);
CREATE INDEX IF NOT EXISTS idx_ventas_rubro ON public.ventas_detalle(rubro);
CREATE INDEX IF NOT EXISTS idx_ventas_comprobante ON public.ventas_detalle(comprobante);

-- Additional indices requested for analytical dimensions
CREATE INDEX IF NOT EXISTS idx_ventas_razon_social ON public.ventas_detalle(razon_social);
CREATE INDEX IF NOT EXISTS idx_ventas_subcanal ON public.ventas_detalle(subcanal);
CREATE INDEX IF NOT EXISTS idx_ventas_chofer ON public.ventas_detalle(chofer);
CREATE INDEX IF NOT EXISTS idx_ventas_segmentoproducto ON public.ventas_detalle(segmentoproducto);
CREATE INDEX IF NOT EXISTS idx_ventas_linea ON public.ventas_detalle(linea);
CREATE INDEX IF NOT EXISTS idx_ventas_reparto ON public.ventas_detalle(reparto);

-- Comment for the AI Agent
COMMENT ON TABLE public.ventas_detalle IS 'Tabla completa con el detalle mensual de ventas. Los nombres de columnas coinciden con el reporte mensual del cliente para facilitar el análisis.';
