package goldencars


import classes.SAR
import com.fasterxml.jackson.databind
import com.fasterxml.jackson.databind.ObjectMapper

package object functions {
  // Campos SAR
  var XMLIn = ""
  var XMLOut = ""

  def transformaJsonSarATablaSar(jsonSar: String) = {
    var jsonSarLimpio = jsonSar.replace("\uFEFF<", "<").replace(">", "/>").replace("//>", "/>").replace("<?xml", "<prueba").replace("?/>", "/>")

    if (jsonSarLimpio.trim.startsWith("</") && jsonSarLimpio.endsWith("/>") ) {
      jsonSarLimpio = jsonSarLimpio.replace("</", "<")
    }

    val jsonSarUnaLinea = jsonSarLimpio.replaceAll("\n", "")
    val mapeadorJackson: ObjectMapper = new ObjectMapper
    val elementosMensajeJson: databind.JsonNode = mapeadorJackson.readTree(jsonSarUnaLinea)

    extraeCamposDeJson(elementosMensajeJson)

    // ES UN PARSEADOR XML DE ANDAR POR CASA, REEMPLAZALO POR UNO MEJOR
    // val Code = XMLIn.substring(XMLIn.indexOf("<Code/>") + "<Code/>".size, XMLIn.indexOf("</Code/>"))
    // val UMID = XMLOut.substring(XMLOut.indexOf("<UMID/>") + "<UMID/>".size, XMLOut.indexOf("</UMID/>"))
    val Code = "codigo1"
    val UMID = "UMID111"

    SAR(Code, UMID)
  }

	def extraeCamposDeJson(nodoPadre: databind.JsonNode): Any = {
		if (nodoPadre.isArray) { // Si el nodo comienza con [
			val nodos = nodoPadre.elements
			while (nodos.hasNext) {
				val nodo = nodos.next
				if (nodo.isObject || nodo.isArray)
					extraeCamposDeJson(nodo)
			}
		} else if (nodoPadre.isObject) { // Si el nodo comienza con {
			val iter = nodoPadre.fieldNames
			while (iter.hasNext) {
				val clave = iter.next
				val contenido = nodoPadre.path(clave)

				clave match {
          // Campos SAR
          case "XMLIn" => XMLIn = contenido.asText()
          case "XMLOut" => XMLOut = contenido.asText()


					case _ => println("No he encontrado la clave %s cuyo valor es %s".format(clave, contenido))
				}

				// Por si en algún momento se generan mensajes Json anidados ...
				if (contenido.isObject || contenido.isArray)
					extraeCamposDeJson(contenido)
			}
		}
	}


}
