import logging
from flask import Flask, request, jsonify, Response, send_file
from google.cloud import storage
from google.cloud import speech_v1p1beta1 as speech
from pydub import AudioSegment
from google.cloud.speech_v1p1beta1 import RecognitionConfig, RecognitionAudio, LongRunningRecognizeRequest
from google.api_core.exceptions import GoogleAPICallError, RetryError
import os
import subprocess

# Configuración básica del logger
logging.basicConfig(
        level=logging.DEBUG,  # Cambia a INFO o WARNING en producción si quieres menos verbosidad
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

project_id = "hardy-album-440814-i1"

# Cliente de Google Cloud Storage
storage_client = storage.Client(project=project_id)


def convertir_a_wav(bucket_name, filename):
    try:
        logger.info(f"Iniciando conversión de archivo {filename} en bucket {bucket_name}")
        # Define la ruta temporal para el archivo descargado
        temp_input = "/tmp/" + filename

        # Descarga el archivo del bucket de Google Cloud Storage
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(filename)
        blob.download_to_filename(temp_input)
        logger.debug(f"Archivo descargado a {temp_input}")

        # Genera el nombre del archivo de salida con '_converted' al final
        output_filename = filename.rsplit('.', 1)[0] + '_converted.wav'
        temp_output = "/tmp/" + output_filename

        subprocess.call(['ffmpeg', '-y', '-i', temp_input, '-acodec', 'pcm_s16le', '-vn', '-f', 'wav', temp_output])

        # Convierte el archivo de entrada a WAV usando pydub
        #audio = AudioSegment.from_file(temp_input).set_channels(1).set_sample_width(2)
        #audio.export(temp_output, format="wav")
        logger.info(f"Archivo convertido guardado como {temp_output}")

        return output_filename
    except Exception as e:
        logger.error(f"Error en la conversión de {filename}: {str(e)}")
        raise


@app.route('/transcribir_audio', methods=['POST'])
def transcribir_audio():
    try:
        logger.info("Solicitud de transcripción recibida")
        # Extrae el URI del archivo de audio desde el JSON de la solicitud
        request_json = request.get_json(silent=True)
        if not request_json or 'filename' not in request_json:
            logger.warning("Faltan parámetros en la solicitud")
            return jsonify({"error": "El campo 'filename' es requerido"}), 400

        filename = request_json['filename']
        filename_no_extension = filename.split('.')[0]
        bucket_name = 'famanzur-speech-to-text-files'

        logger.debug(f"Nombre del archivo recibido: {filename}")
        logger.info(f"Iniciando transcripción para {filename}")

        # Convierte el archivo a WAV
        converted_wav = convertir_a_wav(bucket_name, filename)
        gcs_uri_converted = 'gs://' + bucket_name + '/' + converted_wav

        # Configura el origen del audio
        audio = RecognitionAudio(uri=gcs_uri_converted)

        # # Leer el archivo local en memoria
        # with open(f"/tmp/{converted_wav}", "rb") as audio_file:
        #     audio_content = audio_file.read()
        #     logger.debug("Archivo de audio leído exitosamente")

        # Configura el origen del audio
        # audio = RecognitionAudio(content=audio_content)

        # Configura los parámetros de reconocimiento
        config = RecognitionConfig(
            encoding=RecognitionConfig.AudioEncoding.LINEAR16,
            sample_rate_hertz=8000,
            language_code="es-ES",
            enable_automatic_punctuation=True
        )
        logger.info("Configuración de reconocimiento lista")

        # Cliente de Google Cloud Speech
        speech_client = speech.SpeechClient()

        # Crea la solicitud de reconocimiento
        recognize_request = LongRunningRecognizeRequest(config=config, audio=audio)
        logger.debug("Solicitud de reconocimiento creada")


        # Envía la solicitud de reconocimiento
        logger.info("Enviando solicitud de transcripción a Google Cloud Speech API")
        operation = speech_client.long_running_recognize(request=recognize_request)
        logger.info("Esperando resultados de transcripción...")

        response = operation.result(timeout=180)
        logger.info("Transcripción completada")

        # Procesar los resultados con diarización
        diarized_text = []
        for result in response.results:
            for alternative in result.alternatives:
                segment = {
                    "transcript": alternative.transcript,
                    "words": []
                }
                for word_info in alternative.words:
                    segment["words"].append({
                        "word": word_info.word,
                        "speaker_tag": word_info.speaker_tag
                    })
                diarized_text.append(segment)

        # Ruta temporal para Cloud Run
        output_file_path = "/tmp/" + filename_no_extension + ".txt"

        # Escribir cada transcript en una línea del archivo temporal
        with open(output_file_path, "w", encoding="utf-8") as f:
            for item in diarized_text:
                f.write(item["transcript"] + "\n")
        logger.info(f"Resultados guardados en {output_file_path}")

        # Subir el archivo temporal al bucket
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(filename_no_extension + '.txt')
        blob.upload_from_filename(output_file_path, content_type="application/json; charset=utf-8")
        logger.info(f"Archivo subido a bucket: {bucket_name}/{filename_no_extension}.txt")

        return send_file(output_file_path, as_attachment=True)

    except GoogleAPICallError as e:
        logger.error(f"Error en la llamada a la API de Google: {str(e)}")
        return jsonify({"error": f"Error en la llamada a la API de Google: {str(e)}"}), 500
    except RetryError as e:
        logger.error(f"Error de reintento: {str(e)}")
        return jsonify({"error": f"Error de reintento: {str(e)}"}), 500
    except Exception as e:
        logger.error(f"Error desconocido: {str(e)}")
        return jsonify({"error": f"Error desconocido: {str(e)}"}), 500


if __name__ == '__main__':
    logger.info("Iniciando servidor Flask")
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
