# NO OLVIDAR ESTAS CORRECCIONES DEL PROFESOR | TP integrador
### Puntos clave:
- parámetro `if_exist` en `.to_sql()` de **pandas**
    - separar funciones para increment. y estat...
- aclarar que se aplicó recomendación de partición solo por fecha en increment. y sin partición por ciudad en estát.
- 



## Mensajes completos

### Primera entrega
Hola Gabriel ¿Cómo estás? Espero te encuentres muy bien.

Tu trabajo está excelente, se nota que sabes trabajar muy bien con Python. Python es un lenguaje muy utilizado en Ingeniería de datos en este momento. Sin embargo, entrando a hilar fino en lo que es la Ingeniería de Datos, una de las características principales que deben tener los programas es la simplicidad.


Esta muy buena la interfaz por consola que hiciste, normalmente aprovechamos interfaces, métodos o librerías ya existentes. Por ejemplo herramientas como Airflow o Prefect toman nuestro programa en Python y lo ejecutan de forma automática con cierta periodicidad y en el orden o secuencia que establezcamos. La idea de aprovechar herramientas existentes es para enfocarnos en aspectos como comprender la fuente de datos y la lógica necesaria para enriquecer esos datos y generar información valiosa para el negocio.

Esto tiene una explicación: la prioridad es tener datos certeros, confiables y en el menor tiempo, optimizando el uso de recursos posibles. Tu código esta excelente, no hay duda de ello, tené en cuenta que el programa seguirá creciendo ya que iremos agregando funcionalidades en las próximas entregas.


Si tenés alguna pregunta o necesitas orientación adicional, no dudes en consultarme. Estoy para ayudarte en lo que necesites.

¡Muy buen trabajo! ¡Seguí así!

Tu trabajo queda aprobado. ¡Felicitaciones!

¡Mucho éxito!

### Segunda entrega
Hola Gabriel, ¿Cómo estás? Espero te encuentres muy bien.

Muy buen trabajo, felicitaciones.

El control para evitar la inserción de registros duplicados es una práctica esencial en Data Engineering, genial que la hayas considerado e implementado.

Con respecto al guardado en parquet:

En el caso de particionar por fecha, hay que evaluar la granularidad. En la clase les mostré particionado por fecha y hora. Si el volúmen de datos es pequeño, no se recomienda particionar por hora, solo por fecha o incluso por mes. Si el volúmen fuese muy pequeño, cada partición (o directorio) tendrá muy pocos registros, lo cuál no es óptimo ya que esta estrategia de particionado aplica para grandes volúmenes de datos.

Genial que hayas particionado por ciudad también. Ahora bien, esa partición poseerá un solo registro ya que es un dato estático. Tené en cuenta que en un entorno laboral, no sea necesario aplicar el particionado por el poco volúmen de datos, por mas que vayas agregando otras ciudades. Al particionar por campos categóricos como ciudad, hay que evaluar que la distribución de registros entre cada partición sea uniforme. En este caso, las ventajas se aprecian con grandes volúmenes de datos, más de 1 millón de registros por ejemplo.

En línea con el ítem anterior, estaría bueno que los datos a almacenar en la ruta “regmeteor” posean una columna que haga referencia a la ciudad. Si bien solo obtenes datos para la ciudad de La Plata, en caso de escalar a más ciudades. será necesario determinar a qué ciudad pertenece cada registro. En vez del nombre la ciudad, podrías usar el id.


Seguimos en contacto ante cualquier duda o comentario.

Seguí así! Tu trabajo queda aprobado. Felicitaciones!


¡Mucho éxito!

### Tercera entrega
Hola Gabriel. Cómo estás? Espero que muy bien.


Muy buen avance, aplicaste diferentes tareas para la transformación de datos y realizaste la carga a la base de datos OLAP de forma correcta. Con respecto a lo último, recordá que la opción `if_exists=replace` de Pandas puede ser perjudicial ya que elimina la tabla antes de cargar los datos, lo cual nos puede llevar a perder datos. Además, la tabla es creada de nuevo por Pandas a tal punto de no usar los tipos de datos correctos


No hay problema que no hayas llegado con la implementación de la SCD, no es obligatoria. Podés implementarlo para la entrega final, sino no hay problema, desde ya te anticipo que tu trabajo está en condiciones de aprobar.


Esta es la última entrega parcial del trabajo integrador. En primer lugar, muchas gracias por el esfuerzo realizado en cada entrega. He notado mucho entusiasmo de tu parte en el desarrollo de este trabajo, espero haya sido una experiencia muy enriquecedora.

En los próximos días se habilitará la entrega final del trabajo integrador, esa entrega definirá la nota final del curso, la cual se verá reflejada en el certificado de aprobación.

**Tené en cuenta, por favor, todos las correcciones sugeridas en todas las entregas con el fin de obtener una buena nota.**


No dudes en consultar cualquier cosa.


Seguimos en contacto. Gracias!