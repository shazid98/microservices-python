import pika
import json
import tempfile
import os
from bson.objectid import ObjectId
import moviepy.editor


def start(message, fs_video, fs_mp3s, channel):
  message = json.loads(message)

  # empty temp file
  tf = tempfile.NamedTemporaryFile()
  # video contents
  out = fs_video.get(ObjectId(message['video_fid']))
  # write video contents to temp file
  tf.write(out.read())
  # create audio file from temp video file
  audio = moviepy.editor.VideoFileClip(tf.name).audio
  # close temp file
  tf.close()

  # write audio to the file
  tf_path = tempfile.gettempdir() + f"/{message['video_fid']}.mp3"
  audio.write_audiofile(tf_path)

  # save the file to mongo
  f = open(tf_path, 'rb')
  data = f.read()
  fid = fs_mp3s.put(data)
  f.close()
  os.remove(tf_path)

  # update the video document
  message['mp3_fid'] = str(fid)

  try:
    channel.basic_publish(
      exchange='',
      routing_key=os.environ.get("MP3_QUEUE"),
      body=json.dumps(message),
      properties=pika.BasicProperties(
        delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
      )
    )
  except Exception as err:
    fs_mp3s.delete(fid)
    return "failed to publish message to mp3 queue"