from google.cloud import storage
import logging
import ffmpeg
import os
import glob


#  This fires on metadata change, to cause it
#  gsutil setmeta -h "x-goog-meta-mp3converted:true" gs://bucket/object
#  or by a wildcard
#  #  gsutil -m setmeta -h "x-goog-meta-mp3converted:true"  gs://audio-stream-files/stripped/KIMN-FM/*_2020-05*aac*
def convert_to_mp3(data, context):
    logging.info('{0}:: starting ffmpeg conversion on metadata update'.format(data['name']))
    __convert(data, context)
    return


def convert_to_mp3_create(data, context):
    logging.info('{0}:: starting ffmpeg conversion on file create'.format(data['name']))
    __convert(data, context)
    return


def segment(data, context):
    logging.info('{0}:: segmenting ffmpeg on metadata update'.format(data['name']))
    __segment(data, context)
    return


def segment_create(data, context):
    logging.info('{0}:: segmenting ffmpeg on file create'.format(data['name']))
    __segment(data, context)
    return


def __segment(data, context):
    client = storage.Client().get_bucket('cfr-turk-files')
    segments_client = storage.Client().get_bucket('cfr_segments')
    file_name = data['name']

    logging.info(
        '{0}:: starting segmenting mp3 , data is {1} class is {2}'.format(file_name, data, data.__class__))

    if __skip_segmenting(file_name, data.get('metadata', {})):
        logging.info("{0}:: -- Skipping segmenting".format(file_name))
        return

    mp3_folder = file_name.split('/')[-2]
    mp3_file = file_name.split('/')[-1]
    mp3_prefix = mp3_file.split('.')[0]

    cfr_blob = client.get_blob(file_name)
    tmp_segment = file_name.split("/")[-1]

    logging.info(
        'mp3_folder {0}::  mp3_file is {1} mp3_prefix is {2} '.format(mp3_folder, mp3_file, mp3_prefix))

    __clean_temp(tmp_segment)



    with open(f'/tmp/{tmp_segment}', 'wb') as file:
        cfr_blob.download_to_file(file)

    logging.info("here segmenting {0}".format(tmp_segment))
    try:
        ffmpeg.input(f'/tmp/{tmp_segment}').output(f'/tmp/{mp3_prefix}_%03d.mp3',
                                                   audio_bitrate='192k',
                                                   f='segment',
                                                   segment_time=__segment_seconds()).run(
            capture_stdout=True,
            capture_stderr=True,
            overwrite_output=True)
    except ffmpeg.Error as e:
        logging.error(f'{file_name} stderr {e.stderr.decode("utf8")}')
        raise e

    logging.info("divided file {0} into segments".format(tmp_segment))
    path = f'/tmp/{mp3_prefix}_*.mp3'


    for filename in glob.glob(path):
        logging.info("uploading the segment at {0}".format(filename))
        duration = ffmpeg.probe(filename)['format']['duration']
        if float(duration) > 540:
            mp3_blob = segments_client.blob(f'{mp3_folder}/{os.path.basename(filename)}')
            mp3_blob.upload_from_filename(f'{filename}', 'audio/mpeg')

    for filename in glob.glob(path):
        __clean_temp(os.path.basename(filename))
    __clean_temp(tmp_segment)

    return


def __convert(data, context):
    client = storage.Client().get_bucket('audio-stream-files')
    bucket_name = data['bucket']
    file_name = data['name']

    logging.info(
        '{0}:: starting convert_to_mp3 (192k) , data is {1} class is {2}'.format(file_name, data, data.__class__))

    if __skip_convert(file_name, data.get('metadata', {})):
        logging.info("{0}:: -- Skipping conversion".format(file_name))
        return

    replace = ''
    if file_name.endswith('.aac'):
        mp3_file = file_name.replace('.aac', '.mp3')
    elif file_name.endswith('.out'):
        mp3_file = file_name.replace('.out', '.mp3')
    else:
        mp3_file = file_name

    mp3_folder = mp3_file.split('/')[-2]
    mp3_file = mp3_file.split('/')[-1]

    aac_blob = client.get_blob(file_name)
    tmp_striped = file_name.split("/")[-1]

    logging.info(
        '{0}::  aac_blob is {1} name is {2} size is {3} tmp_acc is {4}'.format(file_name, aac_blob.path, aac_blob.name,
                                                                               aac_blob.size, tmp_striped))
    __clean_temp(tmp_striped)
    __clean_temp(mp3_file)

    with open(f'/tmp/{tmp_striped}', 'wb') as file:
        aac_blob.download_to_file(file)
    logging.info('{0}:: starting ffmpeg conversion'.format(file_name))
    try:
        ffmpeg.input(f'/tmp/{tmp_striped}').output(f'/tmp/{mp3_file}', audio_bitrate='192k', format='mp3').run(
            capture_stdout=True,
            capture_stderr=True,
            overwrite_output=True)
    except ffmpeg.Error as e:
        logging.info('{0} stdout {1}', format(file_name, e.stdout.decode('utf8')))
        logging.info('{0} stderr: {1}', format(file_name, e.stderr.decode('utf8')))
        raise e

    logging.info('{0}:: successfully converted {1} creating blob from {2}'.format(file_name, file_name,
                                                                                  f'/stripped/{mp3_folder}/{mp3_file}'))
    mp3_blob = client.blob(f'stripped/{mp3_folder}/{mp3_file}')
    mp3_blob.upload_from_filename(f'/tmp/{mp3_file}', 'audio/mpeg')
    mp3_blob.metadata = {'mp3converted': True}
    mp3_blob.patch()
    mp3_blob.make_public()
    logging.info('{0}:: function complete'.format(file_name))
    __clean_temp(tmp_striped)
    __clean_temp(mp3_file)

    return


def __clean_temp(filename):
    if os.path.exists(f'/tmp/{filename}'):
        os.remove(f'/tmp/{filename}')


def __skip_convert(file_name, metadata):
    folder = file_name.split('/')[0]
    if folder != 'stripped':
        return True
    if not file_name.endswith('.aac') and not file_name.endswith('.out'):
        return True
    converted = metadata.get('mp3converted')
    return converted is not None


def __skip_segmenting(file_name, metadata):
    folder = file_name.split('/')[0]
    if folder != 'compressed':
        return True
    if not file_name.endswith('.mp3'):
        return True
    return False


def __segment_seconds():
    return os.environ.get('segment_seconds', 900)

#  gsutil -m setmeta -h "x-goog-meta-mp3converted:true"  gs://audio-stream-files/stripped/KIMN-FM/*_2020-05*aac*
# gsutil -m setmeta -h "x-goog-meta-mp3convertstart:true"  gs://audio-stream-files/stripped/KALC-FM/*_2020-05-08*aac*

